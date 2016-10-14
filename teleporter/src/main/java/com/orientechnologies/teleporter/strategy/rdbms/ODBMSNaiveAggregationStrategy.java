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
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguredVertexClass;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.factory.OMapperFactory;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.importengine.rdbms.graphengine.OGraphEngineForDB;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.OAggregatorEdge;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OClassMapper;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.model.graphmodel.OGraphModel;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.persistence.util.OQueryResult;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

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
  protected OConfigurationHandler buildConfigurationHandler() {
    return new OConfigurationHandler(true);
  }

  @Override
  public OER2GraphMapper createSchemaMapper(OSourceDatabaseInfo sourceDBInfo, String outOrientGraphUri, String chosenMapper, String xmlPath, ONameResolver nameResolver,
                                            ODBMSDataTypeHandler handler, List<String> includedTables, List<String> excludedTables, ODocument migrationConfig,
                                            OConfigurationHandler configHandler) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OER2GraphMapper mapper = (OER2GraphMapper) mapperFactory.buildMapper(chosenMapper, sourceDBInfo, xmlPath, includedTables, excludedTables, migrationConfig, configHandler);

    // Step 1: DataBase schema building
    mapper.buildSourceDatabaseSchema();
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getOutputManager().info("\n");
    OTeleporterContext.getInstance().getOutputManager().debug("\n%s\n", ((OER2GraphMapper)mapper).getDataBaseSchema().toString());

    // Step 2: Graph model building
    mapper.buildGraphModel(nameResolver);
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getOutputManager().info("\n");
    OTeleporterContext.getInstance().getOutputManager().debug("\n%s\n", ((OER2GraphMapper)mapper).getGraphModel().toString());

    // Step 3: Eventual migrationConfigDoc applying
    mapper.applyImportConfiguration();

    // Step 4: Aggregation
    ((OER2GraphMapper)mapper).performAggregations();
    OTeleporterContext.getInstance().getOutputManager().debug("\n'Junction-Entity' aggregation complete.\n");
    OTeleporterContext.getInstance().getOutputManager().debug("\n%s\n", ((OER2GraphMapper)mapper).getGraphModel().toString());

    // Step 5: Writing schema on OrientDB
    OGraphModelWriter graphModelWriter = new OGraphModelWriter();
    OGraphModel graphModel = ((OER2GraphMapper)mapper).getGraphModel();
    boolean success = graphModelWriter.writeModelOnOrient(graphModel, handler, outOrientGraphUri);
    if(!success) {
      OTeleporterContext.getInstance().getOutputManager().error("Writing not complete. Something gone wrong.\n");
      throw new OTeleporterRuntimeException();
    }
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getOutputManager().debug("\nOrientDB Schema writing complete.\n");
    OTeleporterContext.getInstance().getOutputManager().info("\n");

    return mapper;
  }


  @Override
  public void executeImport(OSourceDatabaseInfo sourceDBInfo, String outOrientGraphUri, OSource2GraphMapper genericMapper, ODBMSDataTypeHandler handler) {

    try {

      OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();
      statistics.startWork4Time = new Date();
      statistics.runningStepNumber = 4;

      OER2GraphMapper mapper = (OER2GraphMapper) genericMapper;
      ODBQueryEngine dbQueryEngine = OTeleporterContext.getInstance().getDbQueryEngine();
      OGraphEngineForDB graphEngine = new OGraphEngineForDB((OER2GraphMapper)mapper, handler);

      // OrientDB graph initialization/connection
      OrientBaseGraph orientGraph = null;
      OrientGraphFactory factory = new OrientGraphFactory(outOrientGraphUri,"admin","admin");
      orientGraph = factory.getNoTx();
      orientGraph.getRawGraph().declareIntent(new OIntentMassiveInsert());
      orientGraph.setStandardElementConstraints(false);

      // Importing from Entities belonging to hierarchical bags
      super.importEntitiesBelongingToHierarchies(dbQueryEngine, graphEngine, orientGraph);

      // Importing from Entities NOT belonging to hierarchical bags NOR corresponding to join tables
      for (OVertexType currentOutVertexType : mapper.getVertexType2classMappers().keySet()) {

        List<OClassMapper> classMappers = ((OER2GraphMapper)super.mapper).getClassMappersByVertex(currentOutVertexType);
        List<OEntity> mappedEntities = new LinkedList<OEntity>();

        // checking condition
        boolean allEntitiesNotAggregableAndNotBelongingToHierarchies = true;
        for(OClassMapper classMapper: classMappers) {
          OEntity currentEntity = classMapper.getEntity();
          if(currentEntity.getHierarchicalBag() != null || currentEntity.isAggregableJoinTable()) {
            allEntitiesNotAggregableAndNotBelongingToHierarchies = false;
            break;
          }
          else {
            mappedEntities.add(currentEntity);
          }
        }

        if (allEntitiesNotAggregableAndNotBelongingToHierarchies) {

          String aggregationColumns[][] = null;

          //  classes' aggregation case
          if(mappedEntities.size() > 1) {
            OConfiguredVertexClass configuredVertex = mapper.getMigrationConfig().getVertexByMappedEntities(mappedEntities);
            aggregationColumns = super.buildAggregationColumnsFromAggregatedVertex(configuredVertex);
          }

          super.importRecordsIntoVertexClass(mappedEntities, aggregationColumns, currentOutVertexType, dbQueryEngine, graphEngine, orientGraph);
        }
      }

      // Importing from Entities NOT belonging to hierarchical bags and corresponding to join tables
      for (OVertexType currentOutVertexType : mapper.getVertexType2classMappers().keySet()) {

        List<OClassMapper> classMappers = ((OER2GraphMapper)super.mapper).getClassMappersByVertex(currentOutVertexType);
        List<OEntity> mappedEntities = new LinkedList<OEntity>();

        // checking condition
        boolean allEntitiesAggregableAndNotBelongingToHierarchies = true;
        for(OClassMapper classMapper: classMappers) {
          OEntity currentEntity = classMapper.getEntity();
          if(currentEntity.getHierarchicalBag() != null || !currentEntity.isAggregableJoinTable()) {
            allEntitiesAggregableAndNotBelongingToHierarchies = false;
            break;
          }
          else {
            mappedEntities.add(currentEntity);
          }
        }

        // join tables are not aggregable with other join tables, so for each vertex type we can have just one join table
        if(mappedEntities.size() > 1) {
          OTeleporterContext.getInstance().getOutputManager().error("The '%s' vertex type is mapped with several join tables: you cannot aggregate multiple join tables.");
          break;
        }

        if (allEntitiesAggregableAndNotBelongingToHierarchies) {
          this.importJoinTableRecordIntoEdgeClass(mappedEntities, dbQueryEngine, graphEngine, orientGraph);
        }
      }

      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      orientGraph.shutdown();
      OTeleporterContext.getInstance().getOutputManager().info("\n");

    } catch(OTeleporterRuntimeException e) {
      throw e;
    } catch (Exception e){
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "debug");
    }
  }

  protected void importJoinTableRecordIntoEdgeClass(List<OEntity> mappedEntities, ODBQueryEngine dbQueryEngine, OGraphEngineForDB graphEngine, OrientBaseGraph orientGraph) throws SQLException {

    OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();
    OQueryResult queryResult;
    ResultSet records;

    // for each entity in dbSchema all records are retrieved

    //if(handler.geospatialImplemented && super.hasGeospatialAttributes(entity, handler)) {
    //  String query = handler.buildGeospatialQuery(entity);
    //  queryResult = dbQueryEngine.executeQuery(query);
    //}

    OEntity joinTable = mappedEntities.get(0);

    queryResult = dbQueryEngine.getRecordsByEntity(joinTable);

    records = queryResult.getResult();
    ResultSet currentRecord = null;

    OAggregatorEdge aggregatorEdge = this.mapper.getAggregatorEdgeByJoinVertexTypeName(this.mapper.getVertexTypeByEntity(joinTable).getName());

    // each record of the join table used to add an edge
    while(records.next()) {
      currentRecord = records;
      graphEngine.upsertAggregatorEdge(orientGraph, currentRecord, joinTable, aggregatorEdge);

      // Statistics updated
      statistics.analyzedRecords++;

    }
    // closing resultset, connection and statement
    queryResult.closeAll();
  }


}
