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

package com.orientechnologies.orient.teleporter.strategy;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.teleporter.context.OTeleporterContext;
import com.orientechnologies.orient.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.orient.teleporter.factory.ODataTypeHandlerFactory;
import com.orientechnologies.orient.teleporter.factory.OMapperFactory;
import com.orientechnologies.orient.teleporter.factory.ONameResolverFactory;
import com.orientechnologies.orient.teleporter.importengine.ODBQueryEngine;
import com.orientechnologies.orient.teleporter.importengine.OGraphDBCommandEngine;
import com.orientechnologies.orient.teleporter.mapper.OER2GraphMapper;
import com.orientechnologies.orient.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.orient.teleporter.model.dbschema.OAttribute;
import com.orientechnologies.orient.teleporter.model.dbschema.OEntity;
import com.orientechnologies.orient.teleporter.model.dbschema.OHierarchicalBag;
import com.orientechnologies.orient.teleporter.model.dbschema.ORelationship;
import com.orientechnologies.orient.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.orient.teleporter.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.orient.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.orient.teleporter.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.orient.teleporter.persistence.util.OQueryResult;
import com.orientechnologies.orient.teleporter.util.OTimeFormatHandler;
import com.orientechnologies.orient.teleporter.writer.OGraphModelWriter;
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

  public void executeStrategy(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, String nameResolverConvention, 
      List<String> includedTables, List<String> excludedTables, OTeleporterContext context) {	

    Date globalStart = new Date(); 

    // Step 1,2,3
    ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
    ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention, context);
    context.getStatistics().runningStepNumber = 1;
    OSource2GraphMapper mapper = this.createSchemaMapper(driver, uri, username, password, outOrientGraphUri, chosenMapper, xmlPath, nameResolver, includedTables, excludedTables, context);

    // Step 4
    this.executeImport(driver, uri, username, password, outOrientGraphUri, mapper, context);
    context.getStatistics().runningStepNumber = -1;

    Date globalEnd = new Date();

    context.getOutputManager().info("\n\nImporting Complete in %s !", OTimeFormatHandler.getHMSFormat(globalStart, globalEnd));
    context.getOutputManager().info(context.getStatistics().toString());

  }

  public OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, ONameResolver nameResolver,
      List<String> includedTables, List<String> excludedTables, OTeleporterContext context) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OSource2GraphMapper mapper = mapperFactory.buildMapper(chosenMapper, driver, uri, username, password, xmlPath, includedTables, excludedTables, context);

    // DataBase schema building
    mapper.buildSourceSchema(context);
    context.getOutputManager().info("");
    context.getOutputManager().debug("%s\n", mapper.getSourceSchema().toString());

    // Graph model building
    mapper.buildGraphModel(nameResolver, context);
    context.getOutputManager().info("");
    context.getOutputManager().debug("%s\n", mapper.getGraphModel().toString());

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


  public void executeImport(String driver, String uri, String username, String password, String outOrientGraphUri, OSource2GraphMapper genericMapper,  OTeleporterContext context) {

    try {

      OTeleporterStatistics statistics = context.getStatistics();
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

        case "table-per-concrete-type": this.tablePerConcreteTypeImport(bag, mapper, dbQueryEngine, graphDBCommandEngine, context);
        break;

        }
      }

      OQueryResult queryResult = null;
      ResultSet records = null;

      // Importing from Entities NOT belonging to hierarchical bags
      for(OEntity entity: mapper.getDataBaseSchema().getEntities()) {

        if(entity.getHierarchicalBag() == null) {

          // for each entity in dbSchema all records are retrieved
          queryResult = dbQueryEngine.getRecordsByEntity(entity.getName(), context);
          records = queryResult.getResult();
          ResultSet currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(entity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {
            // upsert of the vertex
            currentRecord = records;
            currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(currentRecord, currentOutVertexType, null, context);

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
          queryResult.closeAll(context);
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
   * @param 
   * @param context
   */
  protected void tablePerHierarchyImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine, OGraphDBCommandEngine graphDBCommandEngine, OTeleporterContext context) {

    try {

      OTeleporterStatistics statistics = context.getStatistics();

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;  
      OVertexType currentInVertexType = null;  
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;

      String currentDiscriminatorValue;
      Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
      String physicalCurrentEntityName = it.next().getName();
      
      OQueryResult queryResult = null;
      ResultSet records = null;

      for(int i=bag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: bag.getDepth2entities().get(i)) {
          currentDiscriminatorValue = bag.getEntityName2discriminatorValue().get(currentEntity.getName());

          // for each entity in dbSchema all records are retrieved
          queryResult = dbQueryEngine.getRecordsFromSingleTableByDiscriminatorValue(bag.getDiscriminatorColumn(), currentDiscriminatorValue, physicalCurrentEntityName, context);
          records = queryResult.getResult();
          ResultSet currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(currentEntity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {
            // upsert of the vertex
            currentRecord = records;
            currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(currentRecord, currentOutVertexType, null, context);

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
                  String currentArrivalEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntityName, dbQueryEngine, context);
                  currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentArrivalEntityName));
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
          queryResult.closeAll(context);
          queryResult.isAllClosed();

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
   * @param mapper
   * @param dbQueryEngine
   * @param graphDBCommandEngine 
   * @param context
   */
  protected void tablePerTypeImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine, OGraphDBCommandEngine graphDBCommandEngine, OTeleporterContext context) {

    try {

      OTeleporterStatistics statistics = context.getStatistics();

      ResultSet aggregateTableRecords = null;

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;  
      OVertexType currentInVertexType = null;  
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;
      ResultSet records;
      ResultSet currentRecord;
      ResultSet fullRecord;
      
      OQueryResult queryResult1 = null;
      OQueryResult queryResult2 = null;

      Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
      OEntity rootEntity = it.next();

      for(int i=bag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: bag.getDepth2entities().get(i)) {

          // for each entity in dbSchema all records are retrieved
          queryResult1 = dbQueryEngine.getRecordsByEntity(currentEntity.getName(), context);
          records = queryResult1.getResult();
          currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(currentEntity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {
            queryResult2 = dbQueryEngine.buildAggregateTableFromHierarchicalBag(bag, context);
            aggregateTableRecords = queryResult2.getResult();
            
            // upsert of the vertex
            currentRecord = records;

            // lookup in the aggregateTable
            String[] propertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];
            String[] valueOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];
            String[] aggregateTablePropertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];

            for(int k=0; k<propertyOfKey.length; k++) {
              propertyOfKey[k] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(k).getName();
            }

            for(int k=0; k<propertyOfKey.length; k++) {
              valueOfKey[k] = currentRecord.getString(propertyOfKey[k]);
            }

            for(int k=0; k<aggregateTablePropertyOfKey.length; k++) {
              aggregateTablePropertyOfKey[k] = rootEntity.getPrimaryKey().getInvolvedAttributes().get(k).getName();
            }
            // lookup
            fullRecord = this.getFullRecordByAggregateTable(aggregateTableRecords, aggregateTablePropertyOfKey, valueOfKey,context);

            // record imported if is not present in OrientDB
            Set<String> propertiesOfIndex = new LinkedHashSet<String>(Arrays.asList(aggregateTablePropertyOfKey));  // we need the key of the aggregate table, because we are working on it
            if(!graphDBCommandEngine.alreadyFullImportedInOrient(fullRecord, currentOutVertexType, propertiesOfIndex, context)) {

              currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(fullRecord, currentOutVertexType, propertiesOfIndex, context);

              // for each attribute of the entity belonging to the primary key, correspondent relationship is
              // built as edge and for the referenced record a vertex is built (only id)
              for(ORelationship currentRelation: currentEntity.getAllRelationships()) {

                currentParentEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase(currentRelation.getParentEntityName());
                currentInVertexType = null; // reset for the current iteration

                // checking if parent table belongs to a hierarchical bag
                if(currentParentEntity.getHierarchicalBag() == null)
                  currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));

                // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
                else if(!currentEntity.getHierarchicalBag().equals(currentParentEntity.getHierarchicalBag())){
                  propertyOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];
                  valueOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];

                  int index = 0;
                  for(OAttribute foreignAttribute: currentRelation.getForeignKey().getInvolvedAttributes())  {
                    propertyOfKey[index] = currentRelation.getPrimaryKey().getInvolvedAttributes().get(index).getName();
                    valueOfKey[index] = fullRecord.getString((foreignAttribute.getName()));
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
                    String currentArrivalEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, null, dbQueryEngine, context);
                    currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentArrivalEntityName));
                  }
                }

                // if currentInVertexType is null then there isn't a relationship between to records, thus the edge will not be added.
                if(currentInVertexType != null) {
                  edgeType = mapper.getRelationship2edgeType().get(currentRelation);
                  graphDBCommandEngine.upsertReachedVertexWithEdge(fullRecord, currentRelation, currentOutVertex, currentInVertexType, edgeType.getName(), context);
                }
              }

              // Statistics updated
              statistics.importedRecords++;

            }
            
            // closing aggregateTable result
            queryResult2.closeAll(context);
            queryResult2.isAllClosed();

          }
          // closing resultset, connection and statement
          queryResult1.closeAll(context);
          queryResult1.isAllClosed();

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
  protected void tablePerConcreteTypeImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine, OGraphDBCommandEngine graphDBCommandEngine, OTeleporterContext context) {
    try {

      OTeleporterStatistics statistics = context.getStatistics();

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;  
      OVertexType currentInVertexType = null;  
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;
      ResultSet records;
      ResultSet currentRecord;
      
      OQueryResult queryResult = null;

      for(int i=bag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: bag.getDepth2entities().get(i)) {

          // for each entity in dbSchema all records are retrieved
          queryResult = dbQueryEngine.getRecordsByEntity(currentEntity.getName(), context);
          records = queryResult.getResult();
          currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(currentEntity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {

            // upsert of the vertex
            currentRecord = records;

            // record imported if is not present in OrientDB
            String[] propertyOfKey = new String[currentEntity.getPrimaryKey().getInvolvedAttributes().size()];

            for(int k=0; k<propertyOfKey.length; k++) {
              propertyOfKey[k] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(k).getName();
            }

            Set<String> propertiesOfIndex = new LinkedHashSet<String>(Arrays.asList(propertyOfKey));  // we need the key of original table, because we are working on it
            
            if(!graphDBCommandEngine.alreadyFullImportedInOrient(currentRecord, currentOutVertexType, propertiesOfIndex, context)) {
              
              currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(currentRecord, currentOutVertexType, propertiesOfIndex, context);

              // for each attribute of the entity belonging to the primary key, correspondent relationship is
              // built as edge and for the referenced record a vertex is built (only id)
              for(ORelationship currentRelation: currentEntity.getAllRelationships()) {

                currentParentEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase(currentRelation.getParentEntityName());
                currentInVertexType = null; // reset for the current iteration

                // checking if parent table belongs to a hierarchical bag
                if(currentParentEntity.getHierarchicalBag() == null)
                  currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));

                // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
                else if(!currentEntity.getHierarchicalBag().equals(currentParentEntity.getHierarchicalBag())) {
                  propertyOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];
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
                    String currentArrivalEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, null, dbQueryEngine, context);
                    currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentArrivalEntityName));
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

          }
          // closing resultset, connection and statement
          queryResult.closeAll(context);
          queryResult.isAllClosed();

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
   * @param currentParentEntity
   * @param propertyOfKey
   * @param valueOfKey
   * @param physicalArrivalEntityName
   * @param aggregateTableRecords 
   * @param dbQueryEngine
   * @param context
   * @return
   */
  private String searchParentEntityType(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, String physicalArrivalEntityName, ODBQueryEngine dbQueryEngine, OTeleporterContext context) {

    switch(currentParentEntity.getHierarchicalBag().getInheritancePattern()) {

    case "table-per-hierarchy": return searchParentEntityTypeFromSingleTable(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntityName, dbQueryEngine, context);

    case "table-per-type": return searchParentEntityTypeFromSubclassTable(currentParentEntity, propertyOfKey, valueOfKey, dbQueryEngine, context);

    case "table-per-concrete-type": return searchParentEntityTypeFromConcreteTable(currentParentEntity, propertyOfKey, valueOfKey, dbQueryEngine, context);

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
  private String searchParentEntityTypeFromSingleTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, String physicalEntityName, ODBQueryEngine dbQueryEngine, OTeleporterContext context) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String discriminatorColumn = hierarchicalBag.getDiscriminatorColumn();
    String entityName = null;

    try {

      OQueryResult queryResult = dbQueryEngine.getEntityTypeFromSingleTable(discriminatorColumn, physicalEntityName, propertyOfKey, valueOfKey, context);
      ResultSet result = queryResult.getResult();
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
   * @param dbQueryEngine
   * @param context
   * @return
   */
  private String searchParentEntityTypeFromSubclassTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, ODBQueryEngine dbQueryEngine, OTeleporterContext context) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String entityName = null;

    try {

      OQueryResult queryResult = null;
      ResultSet result = null;

      for(int i=hierarchicalBag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: hierarchicalBag.getDepth2entities().get(i)) {

          // Overwriting propertyOfKey with the currentEntity attributes' names
          for(int j=0; j<currentEntity.getPrimaryKey().getInvolvedAttributes().size(); j++) {
            propertyOfKey[j] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(j).getName();
          }

          queryResult = dbQueryEngine.getRecordById(currentEntity.getName(),propertyOfKey, valueOfKey, context);
          result = queryResult.getResult();

          if(result != null) {
            entityName = currentEntity.getName();
            break;
          }
        }

        if(result != null) {
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
   * @param dbQueryEngine
   * @param context
   * @return
   */
  private String searchParentEntityTypeFromConcreteTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, ODBQueryEngine dbQueryEngine, OTeleporterContext context) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String entityName = null;

    try {

      OQueryResult queryResult = null;
      ResultSet result = null;

      for(int i=hierarchicalBag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: hierarchicalBag.getDepth2entities().get(i)) {

          // Overwriting propertyOfKey with the currentEntity attributes' names
          for(int j=0; j<currentEntity.getPrimaryKey().getInvolvedAttributes().size(); j++) {
            propertyOfKey[j] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(j).getName();
          }

          queryResult = dbQueryEngine.getRecordById(currentEntity.getName(),propertyOfKey, valueOfKey, context);
          result = queryResult.getResult();

          if(result != null) {
            entityName = currentEntity.getName();
            break;
          }
        }

        if(result != null) {
          break;
        }
      }

    }catch(Exception e) {
      e.printStackTrace();
    }

    return entityName;
  }


  /**
   * @param aggregateTableRecords 
   * @param propertyOfKey
   * @param valueOfKey
   * @param valueOfKey2 
   * @param context
   * @return
   */
  private ResultSet getFullRecordByAggregateTable(ResultSet aggregateTableRecords, String[] aggregateTablePropertyOfKey, String[] valueOfKey, OTeleporterContext context) {

    ResultSet fullRecord = null;
    String[] currentValueOfKey = new String[aggregateTablePropertyOfKey.length];
    boolean equals;

    try {

      while(aggregateTableRecords.next()) {

        for(int i=0; i<aggregateTablePropertyOfKey.length; i++) {
          currentValueOfKey[i] = aggregateTableRecords.getString(aggregateTablePropertyOfKey[i]);
        }

        equals = true;
        for(int j=0; j<valueOfKey.length; j++) {
          if(!valueOfKey[j].equals(currentValueOfKey[j])) {
            equals = false;
            break;
          }
        }

        if(equals) {
          fullRecord = aggregateTableRecords;
          return fullRecord;
        }

      }
    }catch(Exception e) {
      context.getOutputManager().error(e.getMessage());
      e.printStackTrace();
    }

    return fullRecord;
  }

}
