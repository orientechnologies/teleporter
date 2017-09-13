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
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OEVClassMapper;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * A strategy that performs a "naive" import of the data source. The data source schema is
 * translated semi-directly in a correspondent and coherent graph model.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class ODBMSNaiveStrategy extends OAbstractDBMSImportStrategy {

  public ODBMSNaiveStrategy(String protocol, String serverInitUrl, String dbName) {
    super(protocol, serverInitUrl, dbName);
  }

  @Override
  protected OConfigurationHandler buildConfigurationHandler() {
    return new OConfigurationHandler(false);
  }

  public OER2GraphMapper createSchemaMapper(OSourceDatabaseInfo sourceDBInfo, String chosenMapper, String xmlPath, ONameResolver nameResolver, ODBMSDataTypeHandler handler,
      List<String> includedTables, List<String> excludedTables, OConfiguration migrationConfig) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OER2GraphMapper mapper = (OER2GraphMapper) mapperFactory
        .buildMapper(chosenMapper, sourceDBInfo, xmlPath, includedTables, excludedTables, migrationConfig);

    // Step 1: DataBase schema building
    mapper.buildSourceDatabaseSchema();
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getMessageHandler().info("\n");
    if(OTeleporterContext.getInstance().getMessageHandler().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getMessageHandler().debug("\n%s\n", ((OER2GraphMapper) mapper).getDataBaseSchema().toString());
    }

    // Step 2: Graph model building
    mapper.buildGraphModel(nameResolver);
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getMessageHandler().info("\n");
    if(OTeleporterContext.getInstance().getMessageHandler().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getMessageHandler().debug("\n%s\n", mapper.getGraphModel().toString());
    }

    // Step 3: eventual migrationConfigDoc applying
    mapper.applyImportConfiguration();

    // Step 4: Writing schema on OrientDB
    OGraphModelWriter graphModelWriter = new OGraphModelWriter(migrationConfig);
    boolean success = graphModelWriter.writeModelOnOrient(mapper, handler, dbName, protocol);

    if (!success) {
      OTeleporterContext.getInstance().getMessageHandler().error("Writing not complete. Something gone wrong.\n");
      throw new OTeleporterRuntimeException();
    }
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    if(OTeleporterContext.getInstance().getMessageHandler().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getMessageHandler().debug("\nOrientDB Schema writing complete.\n");
    }
    OTeleporterContext.getInstance().getMessageHandler().info("\n");

    return mapper;
  }

  public void executeImport(OSourceDatabaseInfo sourceDBInfo, String dbName, OSource2GraphMapper genericMapper,
      ODBMSDataTypeHandler handler) {

    ODatabaseDocument orientGraph = null;

    try {

      OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();
      statistics.startWork4Time = new Date();
      statistics.runningStepNumber = 4;

      OER2GraphMapper mapper = (OER2GraphMapper) genericMapper;
      ODBQueryEngine dbQueryEngine = OTeleporterContext.getInstance().getDbQueryEngine();
      OGraphEngineForDB graphEngine = new OGraphEngineForDB((OER2GraphMapper) mapper, handler);

      // OrientDB graph initialization/connection

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

      // Importing from Entities NOT belonging to hierarchical bags
      for (OVertexType currentOutVertexType : mapper.getVertexType2EVClassMappers().keySet()) {

        List<OEVClassMapper> classMappersByVertex = ((OER2GraphMapper) super.mapper)
            .getEVClassMappersByVertex(currentOutVertexType);
        List<OEntity> mappedEntities = new LinkedList<OEntity>();

        // checking condition
        boolean allEntitiesNotBelongingToHierarchies = true;
        for (OEVClassMapper classMapper : classMappersByVertex) {
          OEntity currentEntity = classMapper.getEntity();
          if (currentEntity.getHierarchicalBag() != null) {
            allEntitiesNotBelongingToHierarchies = false;
            break;
          } else {
            mappedEntities.add(currentEntity);
          }
        }

        if (allEntitiesNotBelongingToHierarchies) {

          String aggregationColumns[][] = null;

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
              for (OEVClassMapper classMapper : classMappersByEntity) {
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

      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      orientGraph.close();

      OTeleporterContext.getInstance().getMessageHandler().info("\n");

    } catch (OTeleporterRuntimeException e) {
      throw e;
    } catch (Exception e) {
      String mess = "Exception during records importing.\n";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");

      if (orientGraph != null) {
        orientGraph.close();
      }
      throw new OTeleporterRuntimeException(e);
    }
  }

}
