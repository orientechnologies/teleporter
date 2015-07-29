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
import java.util.Date;
import java.util.List;

import com.orientechnologies.orient.teleporter.context.OTeleporterContext;
import com.orientechnologies.orient.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.orient.teleporter.factory.ODataTypeHandlerFactory;
import com.orientechnologies.orient.teleporter.factory.OMapperFactory;
import com.orientechnologies.orient.teleporter.importengine.ODBQueryEngine;
import com.orientechnologies.orient.teleporter.importengine.OGraphDBCommandEngine;
import com.orientechnologies.orient.teleporter.mapper.OAggregatorEdge;
import com.orientechnologies.orient.teleporter.mapper.OER2GraphMapper;
import com.orientechnologies.orient.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.orient.teleporter.model.dbschema.OEntity;
import com.orientechnologies.orient.teleporter.model.dbschema.OHierarchicalBag;
import com.orientechnologies.orient.teleporter.model.dbschema.ORelationship;
import com.orientechnologies.orient.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.orient.teleporter.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.orient.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.orient.teleporter.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.orient.teleporter.persistence.util.OQueryResult;
import com.orientechnologies.orient.teleporter.writer.OGraphModelWriter;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * A strategy that performs a "naive" import of the data source. The data source schema is
 * translated semi-directly in a correspondent and coherent graph model using an aggregation 
 * policy on the junction tables of dimension equals to 2.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ONaiveAggregationImportStrategy extends ONaiveImportStrategy {	

  public ONaiveAggregationImportStrategy() {}

@Override
  public OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, ONameResolver nameResolver,
      List<String> includedTables, List<String> excludedTables, OTeleporterContext context) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OSource2GraphMapper mapper = mapperFactory.buildMapper(chosenMapper, driver, uri, username, password, xmlPath, includedTables, excludedTables, context);

    // DataBase schema building
    mapper.buildSourceSchema(context);
    context.getOutputManager().info("");
    context.getOutputManager().debug(((OER2GraphMapper)mapper).getDataBaseSchema().toString() + "\n");

    // Graph model building
    mapper.buildGraphModel(nameResolver, context);
    context.getOutputManager().info("");
    context.getOutputManager().debug(((OER2GraphMapper)mapper).getGraphModel().toString() + "\n");

    // Many-to-Many aggregation
    ((OER2GraphMapper)mapper).JoinTableDim2Aggregation(context);
    context.getOutputManager().debug("'Junction-Entity' aggregation complete.\n");
    context.getOutputManager().debug(((OER2GraphMapper)mapper).getGraphModel().toString() + "\n");


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


  @Override
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

        case "table-per-hierarchy": super.tablePerHierarchyImport(bag, mapper, dbQueryEngine, graphDBCommandEngine, context);
        break;

        case "table-per-type": super.tablePerTypeImport(bag, mapper, dbQueryEngine, graphDBCommandEngine, context);
        break;

        case "table-per-concrete-type": super.tablePerConcreteTypeImport(bag, mapper, dbQueryEngine, graphDBCommandEngine, context);
        break;

        }
      }

      OQueryResult queryResult = null;
      ResultSet records = null;

      // Importing from Entities NOT belonging to hierarchical bags and NOT corresponding to join tables
      for(OEntity entity: mapper.getDataBaseSchema().getEntities()) {

        if(!entity.isJoinEntityDim2() && entity.getHierarchicalBag() == null) {

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

      // Importing from Entities NOT belonging to hierarchical bags and corresponding to join tables
      for(OEntity entity: mapper.getDataBaseSchema().getEntities()) {

        if(entity.isJoinEntityDim2() && entity.getHierarchicalBag() == null) {

          // for each entity in dbSchema all records are retrieved
          queryResult = dbQueryEngine.getRecordsByEntity(entity.getName(), context);
          records = queryResult.getResult();
          ResultSet currentRecord = null;

          OAggregatorEdge aggregatorEdge = mapper.getJoinVertex2aggregatorEdges().get(context.getNameResolver().resolveVertexName(entity.getName()));

          // each record of the join table used to add an edge
          while(records.next()) {
            currentRecord = records;
            graphDBCommandEngine.upsertAggregatorEdge(currentRecord, entity, aggregatorEdge, context);

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

    }catch(Exception e){
      e.printStackTrace();
    }
  }

}
