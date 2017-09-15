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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.configuration.api.OConfiguredVertexClass;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.factory.ODataTypeHandlerFactory;
import com.orientechnologies.teleporter.factory.ONameResolverFactory;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.OSourceInfo;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.strategy.OWorkflowStrategy;
import com.orientechnologies.teleporter.util.OFunctionsHandler;
import com.orientechnologies.teleporter.util.OMigrationConfigManager;

import java.util.Date;
import java.util.List;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public abstract class OAbstractDBMSModelBuildingStrategy implements OWorkflowStrategy {

  protected OER2GraphMapper mapper;

  public OAbstractDBMSModelBuildingStrategy() {
  }

  @Override
  public ODocument executeStrategy(OSourceInfo sourceInfo, String outOrientGraphUri, String chosenMapper, String xmlPath,
      String nameResolverConvention, List<String> includedTables, List<String> excludedTables, ODocument migrationConfigDoc) {

    OSourceDatabaseInfo sourceDBInfo = (OSourceDatabaseInfo) sourceInfo;
    Date globalStart = new Date();

    ODataTypeHandlerFactory dataTypeHandlerFactory = new ODataTypeHandlerFactory();
    ODBMSDataTypeHandler handler = (ODBMSDataTypeHandler) dataTypeHandlerFactory.buildDataTypeHandler(sourceDBInfo.getDriverName());
    OConfigurationHandler configurationHandler = this.buildConfigurationHandler();

    /**
     * Building configuration
     */

    boolean keepVerticesCoordinates = true;
    OConfiguration migrationConfig = null;
    if (migrationConfigDoc != null) {

      // Applying filters to starting migrationConfigDoc
      if(includedTables != null && includedTables.size() > 0) {
        configurationHandler.filterAccordingToWhiteList(migrationConfigDoc, includedTables);
      }
      else if(excludedTables != null && excludedTables.size() > 0) {
        configurationHandler.filterAccordingToBlackList(migrationConfigDoc, excludedTables);
      }

      migrationConfig = configurationHandler.buildConfigurationFromJSONDoc(migrationConfigDoc, keepVerticesCoordinates);
    }


    /*
     * Step 1,2
     */

    ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
    ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention);
    OTeleporterContext.getInstance().getStatistics().runningStepNumber = -1;

    this.mapper = this
        .createSchemaMapper(sourceDBInfo, outOrientGraphUri, chosenMapper, xmlPath, nameResolver, handler, includedTables,
            excludedTables, migrationConfig);

    Date globalEnd = new Date();

    OTeleporterContext.getInstance().getMessageHandler()
        .info("\n\nGraph model building complete in %s\n", OFunctionsHandler.getHMSFormat(globalStart, globalEnd));
    OTeleporterContext.getInstance().getMessageHandler().info(this, OTeleporterContext.getInstance().getStatistics().toString());

    // Building Graph Model mapping (for graph rendering too)
    OConfiguration configuredGraph = configurationHandler.buildConfigurationFromMapper(this.mapper);

    if(keepVerticesCoordinates && migrationConfig != null) {
      // update vertices contained in the new just built config adding the correspondent coordinates from the old configuration (if not null!)
      addCoordinatesFromOldConfiguration(configuredGraph, migrationConfig);
    }

    ODocument configuredGraphDoc = configurationHandler.buildJSONDocFromConfiguration(configuredGraph);
    return configuredGraphDoc;
  }

  public abstract OER2GraphMapper createSchemaMapper(OSourceDatabaseInfo sourceDBInfo, String outOrientGraphUri,
      String chosenMapper, String xmlPath, ONameResolver nameResolver, ODBMSDataTypeHandler handler, List<String> includedTables,
      List<String> excludedTables, OConfiguration migrationConfig);

  protected abstract OConfigurationHandler buildConfigurationHandler();

  public void addCoordinatesFromOldConfiguration(OConfiguration newConfig, OConfiguration oldConfig) {

    for(OConfiguredVertexClass currVertexClass: oldConfig.getConfiguredVertices()) {
      Double x = currVertexClass.getX();
      Double y = currVertexClass.getY();
      Double px = currVertexClass.getPx();
      Double py = currVertexClass.getPy();
      Integer fixed = currVertexClass.getFixed();

      OConfiguredVertexClass newVertexClass = newConfig.getVertexClassByName(currVertexClass.getName());
      if(x != null && y != null && px != null && py != null) {
        newVertexClass.setX(x);
        newVertexClass.setY(y);
        newVertexClass.setPx(px);
        newVertexClass.setPy(py);
      }
      if(fixed != null) {
        newVertexClass.setFixed(fixed);
      }
    }

  }

}
