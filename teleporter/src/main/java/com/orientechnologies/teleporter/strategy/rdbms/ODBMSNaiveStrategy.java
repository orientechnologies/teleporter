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
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.factory.OMapperFactory;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.importengine.rdbms.graphengine.OGraphEngineForDB;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OClassMapper;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.model.graphmodel.OGraphModel;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

import java.util.Date;
import java.util.LinkedList;
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

  @Override
  protected OConfigurationHandler buildConfigurationHandler() {
    return new OConfigurationHandler(false);
  }


  public OER2GraphMapper createSchemaMapper(OSourceDatabaseInfo sourceDBInfo, String outOrientGraphUri, String chosenMapper, String xmlPath, ONameResolver nameResolver,
                                            ODBMSDataTypeHandler handler, List<String> includedTables, List<String> excludedTables, ODocument config,
                                            OConfigurationHandler configHandler, OTeleporterContext context) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OER2GraphMapper mapper = (OER2GraphMapper) mapperFactory.buildMapper(chosenMapper, sourceDBInfo, xmlPath, includedTables, excludedTables, config, configHandler, context);

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

    // Step 3: eventual jsonConfiguration applying
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


  public void executeImport(OSourceDatabaseInfo sourceDBInfo, String outOrientGraphUri, OSource2GraphMapper genericMapper, ODBMSDataTypeHandler handler, OTeleporterContext context) {

    try {

      OTeleporterStatistics statistics = context.getStatistics();
      statistics.startWork4Time = new Date();
      statistics.runningStepNumber = 4;

      OER2GraphMapper mapper = (OER2GraphMapper) genericMapper;
      ODBQueryEngine dbQueryEngine = context.getDbQueryEngine();
      OGraphEngineForDB graphEngine = new OGraphEngineForDB((OER2GraphMapper) mapper, handler);

      // OrientDB graph initialization/connection
      OrientBaseGraph orientGraph = null;
      OrientGraphFactory factory = new OrientGraphFactory(outOrientGraphUri, "admin", "admin");
      orientGraph = factory.getNoTx();
      orientGraph.getRawGraph().declareIntent(new OIntentMassiveInsert());
      orientGraph.setStandardElementConstraints(false);

      // Importing from Entities belonging to hierarchical bags
      super.importEntitiesBelongingToHierarchies(dbQueryEngine, graphEngine, orientGraph, context);

      // Importing from Entities NOT belonging to hierarchical bags
      for (OVertexType currentOutVertexType : mapper.getVertexType2classMappers().keySet()) {

        List<OClassMapper> classMappers = ((OER2GraphMapper)super.mapper).getClassMappersByVertex(currentOutVertexType);
        List<OEntity> mappedEntities = new LinkedList<OEntity>();

        // checking condition
        boolean allEntitiesNotBelongingToHierarchies = true;
        for(OClassMapper classMapper: classMappers) {
          OEntity currentEntity = classMapper.getEntity();
          if(currentEntity.getHierarchicalBag() != null) {
            allEntitiesNotBelongingToHierarchies = false;
            break;
          }
          else {
            mappedEntities.add(currentEntity);
          }
        }

        if (allEntitiesNotBelongingToHierarchies) {
          super.importRecordsIntoVertexClass(mappedEntities, currentOutVertexType, dbQueryEngine, graphEngine, orientGraph, context);
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
