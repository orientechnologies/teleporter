/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * OrientDB LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * OrientDB LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from OrientDB LTD.
 * 
 * For more information: http://www.orientdb.com
 */

package com.orientdb.teleporter.strategy.rdbms;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;

import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.context.OTeleporterStatistics;
import com.orientdb.teleporter.exception.OTeleporterRuntimeException;
import com.orientdb.teleporter.factory.OMapperFactory;
import com.orientdb.teleporter.importengine.rdbms.ODBQueryEngine;
import com.orientdb.teleporter.importengine.rdbms.OGraphEngineForDB;
import com.orientdb.teleporter.mapper.OSource2GraphMapper;
import com.orientdb.teleporter.mapper.rdbms.OAggregatorEdge;
import com.orientdb.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientdb.teleporter.model.dbschema.OEntity;
import com.orientdb.teleporter.model.dbschema.OHierarchicalBag;
import com.orientdb.teleporter.model.dbschema.ORelationship;
import com.orientdb.teleporter.model.graphmodel.OEdgeType;
import com.orientdb.teleporter.model.graphmodel.OGraphModel;
import com.orientdb.teleporter.model.graphmodel.OVertexType;
import com.orientdb.teleporter.nameresolver.ONameResolver;
import com.orientdb.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientdb.teleporter.persistence.util.OQueryResult;
import com.orientdb.teleporter.writer.OGraphModelWriter;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
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

public class ODBMSNaiveAggregationStrategy extends ODBMSImportStrategy {	

  public ODBMSNaiveAggregationStrategy() {}


  @Override
  public OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper,
      String xmlPath, ONameResolver nameResolver, ODBMSDataTypeHandler handler, List<String> includedTables, List<String> excludedTables, OTeleporterContext context) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OSource2GraphMapper mapper = mapperFactory.buildMapper(chosenMapper, driver, uri, username, password, xmlPath, includedTables, excludedTables, context);

    // DataBase schema building
    mapper.buildSourceSchema(context);
    context.getStatistics().notifyListeners();
    context.getOutputManager().info("");
    context.getOutputManager().debug("\n%s\n", ((OER2GraphMapper)mapper).getDataBaseSchema().toString());

    // Graph model building
    mapper.buildGraphModel(nameResolver, context);
    context.getStatistics().notifyListeners();
    context.getOutputManager().info("");
    context.getOutputManager().debug("\n%s\n", ((OER2GraphMapper)mapper).getGraphModel().toString());

    // Many-to-Many aggregation
    ((OER2GraphMapper)mapper).JoinTableDim2Aggregation(context);
    context.getOutputManager().debug("\n'Junction-Entity' aggregation complete.\n");
    context.getOutputManager().debug("\n%s\n", ((OER2GraphMapper)mapper).getGraphModel().toString());

    // Saving schema on Orient
    OGraphModelWriter graphModelWriter = new OGraphModelWriter();  
    OGraphModel graphModel = ((OER2GraphMapper)mapper).getGraphModel();
    boolean success = graphModelWriter.writeModelOnOrient(graphModel, handler, outOrientGraphUri, context);
    if(!success) {
      context.getOutputManager().error("Writing not complete. Something gone wrong.\n");
      throw new OTeleporterRuntimeException();
    }
    context.getStatistics().notifyListeners();
    context.getOutputManager().debug("\nOrientDB Schema writing complete.\n");
    context.getOutputManager().info("");

    return mapper;
  }


  @Override
  public void executeImport(String driver, String uri, String username, String password, String outOrientGraphUri, OSource2GraphMapper genericMapper, ODBMSDataTypeHandler handler, OTeleporterContext context) {

    try {

      OTeleporterStatistics statistics = context.getStatistics();
      statistics.startWork4Time = new Date();
      statistics.runningStepNumber = 4;

      OER2GraphMapper mapper = (OER2GraphMapper) genericMapper;
      ODBQueryEngine dbQueryEngine = new ODBQueryEngine(driver, uri, username, password, context);    
      OGraphEngineForDB graphEngine = new OGraphEngineForDB((OER2GraphMapper)mapper, handler);

      // OrientDB graph initialization/connection
      OrientBaseGraph orientGraph = null;
      OrientGraphFactory factory = new OrientGraphFactory(outOrientGraphUri,"admin","admin");
      orientGraph = factory.getNoTx();
      orientGraph.getRawGraph().declareIntent(new OIntentMassiveInsert());
      orientGraph.setStandardElementConstraints(false);

      OVertexType currentOutVertexType = null;  
      OVertexType currentInVertexType = null;  
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;

      // Importing from Entities belonging to hierarchical bags
      for(OHierarchicalBag bag: mapper.getDataBaseSchema().getHierarchicalBags()) {

        switch(bag.getInheritancePattern()) {

        case "table-per-hierarchy": super.tablePerHierarchyImport(bag, mapper, dbQueryEngine, graphEngine, orientGraph, context);
        break;

        case "table-per-type": super.tablePerTypeImport(bag, mapper, dbQueryEngine, graphEngine, orientGraph, context);
        break;

        case "table-per-concrete-type": super.tablePerConcreteTypeImport(bag, mapper, dbQueryEngine, graphEngine, orientGraph, context);
        break;

        }
      }

      OQueryResult queryResult = null;
      ResultSet records = null;

      // Importing from Entities NOT belonging to hierarchical bags NOR corresponding to join tables
      for(OEntity entity: mapper.getDataBaseSchema().getEntities()) {

        if(!entity.isAggregableJoinTable() && entity.getHierarchicalBag() == null) {

          // for each entity in dbSchema all records are retrieved

          if(handler.geospatialImplemented && super.hasGeospatialAttributes(entity, handler)) {
            String query = handler.buildGeospatialQuery(entity, context);
            queryResult = dbQueryEngine.getRecordsByQuery(query, context);
          }
          else {
            queryResult = dbQueryEngine.getRecordsByEntity(entity.getName(), entity.getSchemaName(), context);
          }

          records = queryResult.getResult();
          ResultSet currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(entity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {

            // upsert of the vertex
            currentRecord = records;
            currentOutVertex = (OrientVertex) graphEngine.upsertVisitedVertex(orientGraph, currentRecord, currentOutVertexType, null, context);

            // for each attribute of the entity belonging to the primary key, correspondent relationship is
            // built as edge and for the referenced record a vertex is built (only id)
            for(ORelationship currentRelation: entity.getOutRelationships()) {
              currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));
              edgeType = mapper.getRelationship2edgeType().get(currentRelation);
              graphEngine.upsertReachedVertexWithEdge(orientGraph, currentRecord, currentRelation, currentOutVertex, currentInVertexType, edgeType.getName(), context);
            }   

            // Statistics updated
            statistics.analyzedRecords++;

          }
          // closing resultset, connection and statement
          queryResult.closeAll(context);
        }
      }

      // Importing from Entities NOT belonging to hierarchical bags and corresponding to join tables
      for(OEntity entity: mapper.getDataBaseSchema().getEntities()) {

        if(entity.isAggregableJoinTable() && entity.getHierarchicalBag() == null) {

          // for each entity in dbSchema all records are retrieved

          if(handler.geospatialImplemented && super.hasGeospatialAttributes(entity, handler)) {
            String query = handler.buildGeospatialQuery(entity, context);
            queryResult = dbQueryEngine.getRecordsByQuery(query, context);
          }
          else {
            queryResult = dbQueryEngine.getRecordsByEntity(entity.getName(), entity.getSchemaName(), context);
          }

          records = queryResult.getResult();
          ResultSet currentRecord = null;

          OAggregatorEdge aggregatorEdge = mapper.getJoinVertex2aggregatorEdges().get(context.getNameResolver().resolveVertexName(entity.getName()));

          // each record of the join table used to add an edge
          while(records.next()) {
            currentRecord = records;
            graphEngine.upsertAggregatorEdge(orientGraph, currentRecord, entity, aggregatorEdge, context);

            // Statistics updated
            statistics.analyzedRecords++;

          }
          // closing resultset, connection and statement
          queryResult.closeAll(context);
        }
      }

      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      orientGraph.shutdown();
      context.getOutputManager().info("");

    } catch(Exception e){
      if(e.getMessage() != null)
        context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
      else
        context.getOutputManager().error(e.getClass().getName());

      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      String s = writer.toString();
      context.getOutputManager().debug("\n" + s + "\n");
    }
  }

}
