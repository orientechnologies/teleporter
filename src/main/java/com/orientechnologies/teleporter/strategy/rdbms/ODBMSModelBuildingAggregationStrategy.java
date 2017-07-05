/*
 * Copyright 2016 OrientDB LTD (info--at--orientdb.com)
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

import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.factory.OMapperFactory;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;

import java.util.List;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class ODBMSModelBuildingAggregationStrategy extends OAbstractDBMSModelBuildingStrategy {

  public ODBMSModelBuildingAggregationStrategy() {
  }

  @Override
  public OER2GraphMapper createSchemaMapper(OSourceDatabaseInfo sourceDBInfo, String outOrientGraphUri, String chosenMapper,
      String xmlPath, ONameResolver nameResolver, ODBMSDataTypeHandler handler, List<String> includedTables,
      List<String> excludedTables, OConfiguration migrationConfig) {

    OMapperFactory mapperFactory = new OMapperFactory();
    OER2GraphMapper mapper = (OER2GraphMapper) mapperFactory
        .buildMapper(chosenMapper, sourceDBInfo, xmlPath, includedTables, excludedTables, migrationConfig);

    // Step 1: DataBase schema building
    mapper.buildSourceDatabaseSchema();
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getOutputManager().info("\n");
    if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getOutputManager().debug("\n%s\n", ((OER2GraphMapper) mapper).getDataBaseSchema().toString());
    }

    // Step 2: Graph model building
    mapper.buildGraphModel(nameResolver);
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getOutputManager().info("\n");
    if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getOutputManager().debug("\n%s\n", ((OER2GraphMapper) mapper).getGraphModel().toString());
    }

    // Step 3: Eventual migrationConfigDoc applying
    mapper.applyImportConfiguration();

    // Step 4: Aggregation
    ((OER2GraphMapper) mapper).performAggregations();
    if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getOutputManager().debug("\n'Junction-Entity' aggregation complete.\n");
      OTeleporterContext.getInstance().getOutputManager().debug("\n%s\n", ((OER2GraphMapper) mapper).getGraphModel().toString());
    }

    return mapper;
  }

  @Override
  protected OConfigurationHandler buildConfigurationHandler() {
    return new OConfigurationHandler(true);
  }

}
