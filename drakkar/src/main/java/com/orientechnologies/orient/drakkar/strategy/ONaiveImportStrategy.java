/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.drakkar.strategy;

import java.sql.ResultSet;
import java.util.Date;
import java.util.Iterator;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.ODrakkarStatistics;
import com.orientechnologies.orient.drakkar.factory.ODataTypeHandlerFactory;
import com.orientechnologies.orient.drakkar.factory.OMapperFactory;
import com.orientechnologies.orient.drakkar.factory.ONameResolverFactory;
import com.orientechnologies.orient.drakkar.importengine.ODBQueryEngine;
import com.orientechnologies.orient.drakkar.importengine.OGraphDBCommandEngine;
import com.orientechnologies.orient.drakkar.mapper.OER2GraphMapper;
import com.orientechnologies.orient.drakkar.mapper.OSource2GraphMapper;
import com.orientechnologies.orient.drakkar.model.dbschema.OAttribute;
import com.orientechnologies.orient.drakkar.model.dbschema.OEntity;
import com.orientechnologies.orient.drakkar.model.dbschema.OHierarchicalBag;
import com.orientechnologies.orient.drakkar.model.dbschema.ORelationship;
import com.orientechnologies.orient.drakkar.model.graphmodel.OEdgeType;
import com.orientechnologies.orient.drakkar.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.drakkar.model.graphmodel.OVertexType;
import com.orientechnologies.orient.drakkar.nameresolver.ONameResolver;
import com.orientechnologies.orient.drakkar.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.orient.drakkar.util.OTimeFormatHandler;
import com.orientechnologies.orient.drakkar.writer.OGraphModelWriter;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * A strategy that performs a "naive" import of the data source. The data source schema is
 * translated semi-directly in a correspondent and coherent graph model.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ONaiveImportStrategy implements OImportStrategy {


  public ONaiveImportStrategy() {}

  public void executeStrategy(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, String nameResolverConvention, ODrakkarContext context) {	

    Date globalStart = new Date(); 

    // Step 1,2,3
    ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
    ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention, context);
    context.getStatistics().runningStepNumber = 1;
    OSource2GraphMapper mapper = this.createSchemaMapper(driver, uri, username, password, outOrientGraphUri, chosenMapper, xmlPath, nameResolver, context);

    // Step 4
    this.executeImport(driver, uri, username, password, outOrientGraphUri, mapper, context);
    context.getStatistics().runningStepNumber = -1;

    Date globalEnd = new Date();

    context.getOutputManager().info("\n\nImporting Complete in " + OTimeFormatHandler.getHMSFormat(globalStart, globalEnd) + " !");
    context.getOutputManager().info(context.getStatistics().toString());

  }

  public OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, ONameResolver nameResolver, ODrakkarContext context) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OSource2GraphMapper mapper = mapperFactory.buildMapper(chosenMapper, driver, uri, username, password, xmlPath, context);

    // DataBase schema building
    mapper.buildSourceSchema(context);
    context.getOutputManager().info("");
    context.getOutputManager().debug(mapper.getSourceSchema().toString() + "\n");

    // Graph model building
    mapper.buildGraphModel(nameResolver, context);
    context.getOutputManager().info("");
    context.getOutputManager().debug(mapper.getGraphModel().toString() + "\n");

    // Saving schema on Orient
    ODataTypeHandlerFactory factory = new ODataTypeHandlerFactory();
    ODriverDataTypeHandler handler = factory.buildDataTypeHandler(driver, context);
    OGraphModelWriter graphModelWriter = new OGraphModelWriter();  
    OGraphModel graphModel = ((OER2GraphMapper)mapper).getGraphModel();
    boolean success = graphModelWriter.writeModelOnOrient(graphModel, handler, outOrientGraphUri, context);
    if(!success) {
      context.getOutputManager().error("Writing not complete. Something's gone wrong.\n");
      System.exit(0);
    }
    context.getOutputManager().debug("OrientDB Schema writing complete.");
    context.getOutputManager().info("");

    return mapper;
  }


  public void executeImport(String driver, String uri, String username, String password, String outOrientGraphUri, OSource2GraphMapper genericMapper,  ODrakkarContext context) {

    try {

      ODrakkarStatistics statistics = context.getStatistics();
      statistics.startWork4Time = new Date();
      statistics.runningStepNumber = 4;

      OER2GraphMapper mapper = (OER2GraphMapper) genericMapper;
      ODBQueryEngine dbQueryEngine = new ODBQueryEngine(driver, uri, username, password);    
      OGraphDBCommandEngine graphDBCommandEngine = new OGraphDBCommandEngine(outOrientGraphUri);

      OVertexType currentOutVertexType = null;  
      OVertexType currentInVertexType = null;  
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;

      // Importing from Entities belonging to hierarchical bags
      for(OHierarchicalBag bag: mapper.getDataBaseSchema().getHierarchicalBags()) {

        switch(bag.getInheritancePattern()) {

        case "table-per-hierarchy": this.tablePerHierarchyImport(bag, mapper, dbQueryEngine, graphDBCommandEngine, context);
        break;

        case "table-per-type": this.tablePerTypeImport(bag, mapper, dbQueryEngine, graphDBCommandEngine, context);
        break;

        case "table-per-concrete-type": this.tablePerConcreteTypeImport(bag, dbQueryEngine, context);
        break;

        }
      }


      // Importing from Entities NOT belonging to hierarchical bags
      for(OEntity entity: mapper.getDataBaseSchema().getEntities()) {

        if(entity.getHierarchicalBag() == null){

          // for each entity in dbSchema all records are retrieved
          ResultSet records = dbQueryEngine.getRecordsByEntity(entity.getName(), context);
          ResultSet currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(entity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {
            // upsert of the vertex
            currentRecord = records;
            currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(currentRecord, currentOutVertexType, context);

            // for each attribute of the entity belonging to the primary key, correspondent relationship is
            // built as edge and for the referenced record a vertex is built (only id)
            for(ORelationship currentRelation: entity.getRelationships()) {
              currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));
              edgeType = mapper.getRelationship2edgeType().get(currentRelation);
              graphDBCommandEngine.upsertReachedVertexWithEdge(currentRecord, currentRelation, currentOutVertex, currentInVertexType, edgeType.getName(), context);
            }   

            // Statistics updated
            statistics.importedRecords++;
          }

          // closing resultset, connection and statement
          dbQueryEngine.closeAll(context);
        }
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      context.getOutputManager().info("");

    }catch(Exception e) {
      e.printStackTrace();
    }
  }


  /**
   * Performs import of all records of the entities contained in the hierarchical bag passed as parameter.
   * Adopted in case of "Table per Hierarchy" inheritance strategy.
   * @param bag
   * @param mapper
   * @param dbQueryEngine
   * @param graphDBCommandEngine 
   * @param context
   */
  private void tablePerHierarchyImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine, OGraphDBCommandEngine graphDBCommandEngine, ODrakkarContext context) {

    try {

      ODrakkarStatistics statistics = context.getStatistics();
      
      ResultSet aggregateTableRecords = dbQueryEngine.buildAggregateTableFromHierarchicalBag(bag, context);

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;  
      OVertexType currentInVertexType = null;  
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;

      String currentDiscriminatorValue;
      Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
      String physicalCurrentEntityName = it.next().getName();
      int i;

      for(i=bag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: bag.getDepth2entities().get(i)) {
          currentDiscriminatorValue = bag.getEntityName2discriminatorValue().get(currentEntity.getName());

          // for each entity in dbSchema all records are retrieved
          ResultSet records = dbQueryEngine.getRecordsFromSingleTableByDiscriminatorValue(bag.getDiscriminatorColumn(), currentDiscriminatorValue, physicalCurrentEntityName, context);
          ResultSet currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(currentEntity);
          
          // each record is imported as vertex in the orient graph
          while(records.next()) {
            // upsert of the vertex
            currentRecord = records;
            currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(currentRecord, currentOutVertexType, context);

            // for each attribute of the entity belonging to the primary key, correspondent relationship is
            // built as edge and for the referenced record a vertex is built (only id)
            for(ORelationship currentRelation: currentEntity.getAllRelationships()) {

              currentParentEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase(currentRelation.getParentEntityName());

              // checking if parent table belongs to a hierarchical bag
              if(currentParentEntity.getHierarchicalBag() == null)
                currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));

              // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
              else {
                String[] propertyOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];
                String[] valueOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];

                int index = 0;
                for(OAttribute foreignAttribute: currentRelation.getForeignKey().getInvolvedAttributes())  {
                  propertyOfKey[index] = currentRelation.getPrimaryKey().getInvolvedAttributes().get(index).getName();
                  valueOfKey[index] = currentRecord.getString((foreignAttribute.getName()));
                  index++;
                }

                // search is performed only if all the values in the foreign key are different from null (the relationship is inherited and is also consistent)
                boolean ok = true;

                for(int j=0; j<valueOfKey.length; j++) {
                  if(valueOfKey[j] == null) {
                    ok = false;
                    break;
                  }
                }
                if(ok) {
                  it = currentParentEntity.getHierarchicalBag().getDepth2entities().get(0).iterator();
                  String physicalArrivalEntityName = it.next().getName();
                  String currentEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntityName, dbQueryEngine, context);
                  currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentEntityName));
                }
              }

              // if currentInVertexType is null then there isn't a relationship between to records, thus the edge will not be added.
              if(currentInVertexType != null) {
                edgeType = mapper.getRelationship2edgeType().get(currentRelation);
                graphDBCommandEngine.upsertReachedVertexWithEdge(currentRecord, currentRelation, currentOutVertex, currentInVertexType, edgeType.getName(), context);
              }
            }

            // Statistics updated
            statistics.importedRecords++;
          }
          // closing resultset, connection and statement
          dbQueryEngine.closeAll(context);

        }
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      context.getOutputManager().info("");

    }catch(Exception e) {
      e.printStackTrace();
    }
  }


  /**
   * Performs import of all records of the entities contained in the hierarchical bag passed as parameter.
   * Adopted in case of "Table per Type" inheritance strategy.
   * @param bag
   * @param dbQueryEngine
   * @param context
   */
  private void tablePerTypeImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine, OGraphDBCommandEngine graphDBCommandEngine, ODrakkarContext context) {
    
    try {

      ODrakkarStatistics statistics = context.getStatistics();

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;  
      OVertexType currentInVertexType = null;  
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;
      
      

      String currentDiscriminatorValue;
      Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
      String physicalCurrentEntityName = it.next().getName();
      int i;

      for(i=bag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: bag.getDepth2entities().get(i)) {
          currentDiscriminatorValue = bag.getEntityName2discriminatorValue().get(currentEntity.getName());

          // for each entity in dbSchema all records are retrieved
          ResultSet records = dbQueryEngine.getRecordsFromSingleTableByDiscriminatorValue(bag.getDiscriminatorColumn(), currentDiscriminatorValue, physicalCurrentEntityName, context);
          ResultSet currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(currentEntity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {
            // upsert of the vertex
            currentRecord = records;
            currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(currentRecord, currentOutVertexType, context);

            // for each attribute of the entity belonging to the primary key, correspondent relationship is
            // built as edge and for the referenced record a vertex is built (only id)
            for(ORelationship currentRelation: currentEntity.getAllRelationships()) {

              currentParentEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase(currentRelation.getParentEntityName());

              // checking if parent table belongs to a hierarchical bag
              if(currentParentEntity.getHierarchicalBag() == null)
                currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));

              // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
              else {
                String[] propertyOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];
                String[] valueOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];

                int index = 0;
                for(OAttribute foreignAttribute: currentRelation.getForeignKey().getInvolvedAttributes())  {
                  propertyOfKey[index] = currentRelation.getPrimaryKey().getInvolvedAttributes().get(index).getName();
                  valueOfKey[index] = currentRecord.getString((foreignAttribute.getName()));
                  index++;
                }

                // search is performed only if all the values in the foreign key are different from null (the relationship is inherited and is also consistent)
                boolean ok = true;

                for(int j=0; j<valueOfKey.length; j++) {
                  if(valueOfKey[j] == null) {
                    ok = false;
                    break;
                  }
                }
                if(ok) {
                  it = currentParentEntity.getHierarchicalBag().getDepth2entities().get(0).iterator();
                  String physicalArrivalEntityName = it.next().getName();
                  String currentEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntityName, dbQueryEngine, context);
                  currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentEntityName));
                }
              }

              // if currentInVertexType is null then there isn't a relationship between to records, thus the edge will not be added.
              if(currentInVertexType != null) {
                edgeType = mapper.getRelationship2edgeType().get(currentRelation);
                graphDBCommandEngine.upsertReachedVertexWithEdge(currentRecord, currentRelation, currentOutVertex, currentInVertexType, edgeType.getName(), context);
              }
            }

            // Statistics updated
            statistics.importedRecords++;
          }
          // closing resultset, connection and statement
          dbQueryEngine.closeAll(context);

        }
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      context.getOutputManager().info("");

    }catch(Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * Performs import of all records of the entities contained in the hierarchical bag passed as parameter.
   * Adopted in case of "Table per Concrete Type" inheritance strategy.
   * @param bag
   * @param dbQueryEngine
   * @param context
   */
  private void tablePerConcreteTypeImport(OHierarchicalBag bag, ODBQueryEngine dbQueryEngine, ODrakkarContext context) {
    // TODO Auto-generated method stub

  }

  
  /**
   * @param currentParentEntity
   * @param propertyOfKey
   * @param valueOfKey
   * @param physicalArrivalEntityName
   * @param dbQueryEngine
   * @param context
   * @return
   */
  private String searchParentEntityType(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, String physicalArrivalEntityName, ODBQueryEngine dbQueryEngine, ODrakkarContext context) {

    switch(currentParentEntity.getHierarchicalBag().getInheritancePattern()) {
    
    case "table-per-hierarchy": return searchParentEntityTypeFromSingleTable(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntityName, dbQueryEngine, context);

    case "table-per-type": return searchParentEntityTypeFromSubclassTable(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntityName, dbQueryEngine, context);

    case "table-per-concrete-type": return searchParentEntityTypeFromSubclassTable(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntityName, dbQueryEngine, context);

    }
    
    return null;
  }
  


  /**
   * @param currentEntity
   * @param valueOfKey 
   * @param propertyOfKey 
   * @param physicalEntityName
   * @param dbQueryEngine
   * @param context 
   * @return
   */
  private String searchParentEntityTypeFromSingleTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, String physicalEntityName, ODBQueryEngine dbQueryEngine, ODrakkarContext context) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String discriminatorColumn = hierarchicalBag.getDiscriminatorColumn();
    String entityName = null;

    try {

      ResultSet result = dbQueryEngine.getEntityTypeFromSingleTable(discriminatorColumn, physicalEntityName, propertyOfKey, valueOfKey, context);
      result.next();
      String discriminatorValue = result.getString(discriminatorColumn);

      for(String currentEntityName: hierarchicalBag.getEntityName2discriminatorValue().keySet()) {
        if(hierarchicalBag.getEntityName2discriminatorValue().get(currentEntityName).equals(discriminatorValue)) {
          entityName = currentEntityName;
          break;
        }
      }

    }catch(Exception e) {
      e.printStackTrace();
    }

    return entityName;
  }
  
  
  /**
   * @param currentParentEntity
   * @param propertyOfKey
   * @param valueOfKey
   * @param physicalArrivalEntityName
   * @param dbQueryEngine
   * @param context
   * @return
   */
  private String searchParentEntityTypeFromSubclassTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey,
      String physicalArrivalEntityName, ODBQueryEngine dbQueryEngine, ODrakkarContext context) {
    // TODO Auto-generated method stub
    return null;
  }

}
