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

package com.orientechnologies.teleporter.strategy.rdbms;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.factory.OMapperFactory;
import com.orientechnologies.teleporter.importengine.rdbms.ODBQueryEngine;
import com.orientechnologies.teleporter.importengine.rdbms.OGraphEngineForDB;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;
import com.orientechnologies.teleporter.model.dbschema.ORelationship;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OGraphModel;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.persistence.util.OQueryResult;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.sql.ResultSet;
import java.util.Date;
import java.util.List;

/**
 * A strategy that performs a "naive" import of the data source. The data source schema is
 * translated semi-directly in a correspondent and coherent graph model.
 *
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODBMSNaiveStrategy extends ODBMSImportStrategy {


  public ODBMSNaiveStrategy() {}


  public OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, ONameResolver nameResolver,
      ODBMSDataTypeHandler handler, List<String> includedTables, List<String> excludedTables, ODocument config, OTeleporterContext context) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OSource2GraphMapper mapper = mapperFactory.buildMapper(chosenMapper, driver, uri, username, password, xmlPath, includedTables, excludedTables, config, context);

    // Step 1: DataBase schema building
    mapper.buildSourceDatabaseSchema(context);
    context.getStatistics().notifyListeners();
    context.getOutputManager().info("\n");
    context.getOutputManager().debug("\n%s\n", ((OER2GraphMapper)mapper).getDataBaseSchema().toString());

    // Step 2: Graph model building
    mapper.buildGraphModel(nameResolver, context);
    context.getStatistics().notifyListeners();
    context.getOutputManager().info("\n");
    context.getOutputManager().debug("\n%s\n", mapper.getGraphModel().toString());

    // Step 3: eventual configuration applying
    mapper.applyImportConfiguration(context);

    // Step 4: Writing schema on OrientDB
    OGraphModelWriter graphModelWriter = new OGraphModelWriter();
    OGraphModel graphModel = ((OER2GraphMapper)mapper).getGraphModel();
    boolean success = graphModelWriter.writeModelOnOrient(graphModel, handler, outOrientGraphUri, context);
    if(!success) {
      context.getOutputManager().error("Writing not complete. Something gone wrong.\n");
      throw new OTeleporterRuntimeException();
    }
    context.getStatistics().notifyListeners();
    context.getOutputManager().debug("\nOrientDB Schema writing complete.\n");
    context.getOutputManager().info("\n");

    return mapper;
  }


  public void executeImport(String driver, String uri, String username, String password, String outOrientGraphUri, OSource2GraphMapper genericMapper, ODBMSDataTypeHandler handler, OTeleporterContext context) {

    try {

      OTeleporterStatistics statistics = context.getStatistics();
      statistics.startWork4Time = new Date();
      statistics.runningStepNumber = 4;

      OER2GraphMapper mapper = (OER2GraphMapper) genericMapper;
      ODBQueryEngine dbQueryEngine = new ODBQueryEngine(driver, uri, username, password, context);
      OGraphEngineForDB graphEngine = new OGraphEngineForDB((OER2GraphMapper) mapper, handler);

      // OrientDB graph initialization/connection
      OrientBaseGraph orientGraph = null;
      OrientGraphFactory factory = new OrientGraphFactory(outOrientGraphUri, "admin", "admin");
      orientGraph = factory.getNoTx();
      orientGraph.getRawGraph().declareIntent(new OIntentMassiveInsert());
      orientGraph.setStandardElementConstraints(false);

      OVertexType currentOutVertexType = null;
      OVertexType currentInVertexType = null;
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;

      // Importing from Entities belonging to hierarchical bags
      for (OHierarchicalBag bag : mapper.getDataBaseSchema().getHierarchicalBags()) {

        switch (bag.getInheritancePattern()) {

        case "table-per-hierarchy":
          super.tablePerHierarchyImport(bag, mapper, dbQueryEngine, graphEngine, orientGraph, context);
          break;

        case "table-per-type":
          super.tablePerTypeImport(bag, mapper, dbQueryEngine, graphEngine, orientGraph, context);
          break;

        case "table-per-concrete-type":
          super.tablePerConcreteTypeImport(bag, mapper, dbQueryEngine, graphEngine, orientGraph, context);
          break;

        }
      }

      OQueryResult queryResult = null;
      ResultSet records = null;

      // Importing from Entities NOT belonging to hierarchical bags
      for (OEntity entity : mapper.getDataBaseSchema().getEntities()) {

        if (entity.getHierarchicalBag() == null) {

          // for each entity in dbSchema all records are retrieved

          if (handler.geospatialImplemented && super.hasGeospatialAttributes(entity, handler)) {
            String query = handler.buildGeospatialQuery(entity, context);
            queryResult = dbQueryEngine.getRecordsByQuery(query, context);
          } else {
            queryResult = dbQueryEngine.getRecordsByEntity(entity.getName(), entity.getSchemaName(), context);
          }

          records = queryResult.getResult();
          ResultSet currentRecord = null;

          currentOutVertexType = mapper.getVertexTypeByEntity(entity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {

            // upsert of the vertex
            currentRecord = records;
            currentOutVertex = (OrientVertex) graphEngine.upsertVisitedVertex(orientGraph, currentRecord, currentOutVertexType, null, context);

            // for each attribute of the entity belonging to the primary key, correspondent relationship is
            // built as edge and for the referenced record a vertex is built (only id)
            for(ORelationship currentRelationship: entity.getOutRelationships()) {
              OEntity currentParentEntity = mapper.getDataBaseSchema().getEntityByName(currentRelationship.getParentEntity().getName());
              currentInVertexType = mapper.getVertexTypeByEntity(currentParentEntity);

              edgeType = mapper.getRelationship2edgeType().get(currentRelationship);
              graphEngine.upsertReachedVertexWithEdge(orientGraph, currentRecord, currentRelationship, currentOutVertex, currentInVertexType, edgeType.getName(), context);
            }

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
      context.getOutputManager().info("\n");

    } catch (OTeleporterRuntimeException e) {
      throw e;
    } catch (Exception e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
  }

}
