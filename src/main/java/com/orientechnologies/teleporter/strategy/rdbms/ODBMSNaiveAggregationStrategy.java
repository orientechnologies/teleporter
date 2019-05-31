/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.teleporter.strategy.rdbms;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.output.OOutputStreamManager;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
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
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OEVClassMapper;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.persistence.util.OQueryResult;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;

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
 * @email <g.ponzi--at--orientdb.com>
 */

public class ODBMSNaiveAggregationStrategy extends OAbstractDBMSImportStrategy {

  public ODBMSNaiveAggregationStrategy(String protocol, String serverInitUrl, String dbName) {
    super(protocol, serverInitUrl, dbName);
  }

  @Override
  protected OConfigurationHandler buildConfigurationHandler() {
    return new OConfigurationHandler(true);
  }

  @Override
  public OER2GraphMapper createSchemaMapper(OSourceDatabaseInfo sourceDBInfo, String chosenMapper, String xmlPath, ONameResolver nameResolver, ODBMSDataTypeHandler handler,
      List<String> includedTables, List<String> excludedTables, OConfiguration migrationConfig) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OER2GraphMapper mapper = (OER2GraphMapper) mapperFactory
        .buildMapper(chosenMapper, sourceDBInfo, xmlPath, includedTables, excludedTables, migrationConfig);

    // Step 1: DataBase schema building
    mapper.buildSourceDatabaseSchema();
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getMessageHandler().info(this, "\n");
    if(OTeleporterContext.getInstance().getMessageHandler().getOutputManagerLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getMessageHandler().debug(this, "\n%s\n", ((OER2GraphMapper) mapper).getDataBaseSchema().toString());
    }

    // Step 2: Graph model building
    mapper.buildGraphModel(nameResolver);
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getMessageHandler().info(this, "\n");
    if(OTeleporterContext.getInstance().getMessageHandler().getOutputManagerLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getMessageHandler().debug(this, "\n%s\n", ((OER2GraphMapper) mapper).getGraphModel().toString());
    }

    // Step 3: Eventual migrationConfigDoc applying
    mapper.applyImportConfiguration();

    // Step 4: Aggregation
    ((OER2GraphMapper) mapper).performAggregations();
    if(OTeleporterContext.getInstance().getMessageHandler().getOutputManagerLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getMessageHandler().debug(this, "\n'Junction-Entity' aggregation complete.\n");
      OTeleporterContext.getInstance().getMessageHandler().debug(this, "\n%s\n", ((OER2GraphMapper) mapper).getGraphModel().toString());
    }

    // Step 5: Writing schema on OrientDB
    OGraphModelWriter graphModelWriter = new OGraphModelWriter(migrationConfig);
    boolean success = graphModelWriter.writeModelOnOrient(mapper, handler, dbName, protocol);

    if (!success) {
      OTeleporterContext.getInstance().getMessageHandler().error(this, "Writing not complete. Something gone wrong.\n");
      throw new OTeleporterRuntimeException();
    }
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    if(OTeleporterContext.getInstance().getMessageHandler().getOutputManagerLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getMessageHandler().debug(this, "\nOrientDB Schema writing complete.\n");
    }
    OTeleporterContext.getInstance().getMessageHandler().info(this, "\n");

    return mapper;
  }

  @Override
  public void executeImport(OSourceDatabaseInfo sourceDBInfo, String dbName, OSource2GraphMapper genericMapper,
      ODBMSDataTypeHandler handler) {

    try {

      OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();
      statistics.startWork4Time = new Date();
      statistics.runningStepNumber = 4;

      OER2GraphMapper mapper = (OER2GraphMapper) genericMapper;
      ODBQueryEngine dbQueryEngine = OTeleporterContext.getInstance().getDbQueryEngine();
      OGraphEngineForDB graphEngine = new OGraphEngineForDB((OER2GraphMapper) mapper, handler);

      // OrientDB graph initialization/connection
      ODatabaseDocument orientGraph;
      try {
        orientGraph = OTeleporterContext.getInstance().getOrientDBInstance().open(dbName,"admin","admin");
      } catch (Exception e) {
        String mess = "";
        OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
        OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
        throw new OTeleporterRuntimeException(e);
      }

      // Importing from Entities belonging to hierarchical bags
      super.importEntitiesBelongingToHierarchies(dbQueryEngine, graphEngine, orientGraph);

      // Importing from Entities NOT belonging to hierarchical bags NOR corresponding to join tables
      for (OVertexType currentOutVertexType : mapper.getVertexType2EVClassMappers().keySet()) {

        List<OEVClassMapper> classMappersByVertex = ((OER2GraphMapper) super.mapper)
            .getEVClassMappersByVertex(currentOutVertexType);
        List<OEntity> mappedEntities = new LinkedList<OEntity>();

        // checking condition
        boolean allEntitiesNotAggregableAndNotBelongingToHierarchies = true;
        for (OEVClassMapper classMapper : classMappersByVertex) {
          OEntity currentEntity = classMapper.getEntity();
          if (currentEntity.getHierarchicalBag() != null || currentEntity.isAggregableJoinTable()) {
            allEntitiesNotAggregableAndNotBelongingToHierarchies = false;
            break;
          } else {
            mappedEntities.add(currentEntity);
          }
        }

        if (allEntitiesNotAggregableAndNotBelongingToHierarchies) {

          String[][] aggregationColumns = null;

          //  classes' aggregation case
          if (mappedEntities.size() > 1) {
            OConfiguredVertexClass configuredVertex = mapper.getMigrationConfig().getVertexByMappedEntities(mappedEntities);
            aggregationColumns = super.buildAggregationColumnsFromAggregatedVertex(configuredVertex);
            if (!currentOutVertexType.isAnalyzedInLastMigration()) {
              super
                  .importRecordsFromEntitiesIntoVertexClass(mappedEntities, aggregationColumns, currentOutVertexType, dbQueryEngine,
                      graphEngine, orientGraph);
            }
          } else if (mappedEntities.size() == 1) {

            List<OEVClassMapper> classMappersByEntity = ((OER2GraphMapper) super.mapper)
                .getEVClassMappersByEntity(mappedEntities.get(0));

            // 1-1 mapping
            if (classMappersByEntity.size() == 1) {
              if (!currentOutVertexType.isAnalyzedInLastMigration()) {
                super.importRecordsFromEntitiesIntoVertexClass(mappedEntities, aggregationColumns, currentOutVertexType,
                    dbQueryEngine, graphEngine, orientGraph);
              }
            }

            // splitting case (1-N)
            else if (classMappersByEntity.size() > 1) {
              List<OVertexType> mappedVertices = new LinkedList<OVertexType>();
              for (OEVClassMapper classMapper : classMappersByVertex) {
                mappedVertices.add(classMapper.getVertexType());
              }
              if (!currentOutVertexType.isAnalyzedInLastMigration()) {
                super.importRecordsFromSplitEntityIntoVertexClasses(mappedEntities, mappedVertices, dbQueryEngine, graphEngine,
                    orientGraph);
              }
            }

          }
        }
      }

      // Importing from Entities NOT belonging to hierarchical bags and corresponding to join tables
      for (OVertexType currentOutVertexType : mapper.getVertexType2EVClassMappers().keySet()) {

        List<OEVClassMapper> classMappers = ((OER2GraphMapper) super.mapper).getEVClassMappersByVertex(currentOutVertexType);
        List<OEntity> mappedEntities = new LinkedList<OEntity>();

        // checking condition
        boolean allEntitiesAggregableAndNotBelongingToHierarchies = true;
        for (OEVClassMapper classMapper : classMappers) {
          OEntity currentEntity = classMapper.getEntity();
          if (currentEntity.getHierarchicalBag() != null || !currentEntity.isAggregableJoinTable()) {
            allEntitiesAggregableAndNotBelongingToHierarchies = false;
            break;
          } else {
            mappedEntities.add(currentEntity);
          }
        }

        // join tables are not aggregable with other join tables, so for each vertex type we can have just one join table
        if (mappedEntities.size() > 1) {
          OTeleporterContext.getInstance().getMessageHandler()
              .error(this, "The '%s' vertex type is mapped with several join tables: you cannot aggregate multiple join tables.");
          break;
        }

        if (allEntitiesAggregableAndNotBelongingToHierarchies) {
          this.importJoinTableRecordIntoEdgeClass(mappedEntities, dbQueryEngine, graphEngine, orientGraph);
        }
      }

      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      orientGraph.close();
      OTeleporterContext.getInstance().getMessageHandler().info(this, "\n");

    } catch (OTeleporterRuntimeException e) {
      throw e;
    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "debug");
    }
  }

  protected void importJoinTableRecordIntoEdgeClass(List<OEntity> mappedEntities, ODBQueryEngine dbQueryEngine,
      OGraphEngineForDB graphEngine, ODatabaseDocument orientGraph) throws SQLException {

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

    OAggregatorEdge aggregatorEdge = this.mapper
        .getAggregatorEdgeByJoinVertexTypeName(this.mapper.getVertexTypeByEntity(joinTable).getName());

    // each record of the join table used to add an edge
    while (records.next()) {
      currentRecord = records;
      graphEngine.upsertAggregatorEdge(orientGraph, currentRecord, joinTable, aggregatorEdge);

      // Statistics updated
      statistics.analyzedRecords++;

    }
    // closing resultset, connection and statement
    queryResult.closeAll();
  }

}
