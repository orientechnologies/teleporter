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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
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
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public abstract class ODBMSModelBuildingStrategy implements OWorkflowStrategy {

    protected OER2GraphMapper mapper;

    public ODBMSModelBuildingStrategy() {}

    @Override
    public ODocument executeStrategy(OSourceInfo sourceInfo, String outOrientGraphUri, String chosenMapper, String xmlPath, String nameResolverConvention,
                                     List<String> includedTables, List<String> excludedTables, String jsonMigrationConfig) {

        OSourceDatabaseInfo sourceDBInfo = (OSourceDatabaseInfo) sourceInfo;
        Date globalStart = new Date();

        // configuration building
        ODocument migrationConfig = null;
        if(jsonMigrationConfig != null && jsonMigrationConfig.length() > 0) {
            migrationConfig = new ODocument();
            migrationConfig.fromJSON(jsonMigrationConfig, "noMap");
        }

        ODataTypeHandlerFactory dataTypeHandlerFactory = new ODataTypeHandlerFactory();
        ODBMSDataTypeHandler handler = (ODBMSDataTypeHandler) dataTypeHandlerFactory.buildDataTypeHandler(sourceDBInfo.getDriverName());
        OConfigurationHandler configurationHandler = this.buildConfigurationHandler();

        /*
         * Step 1,2
         */
        ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
        ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention);
        OTeleporterContext.getInstance().getStatistics().runningStepNumber = -1;

        this.mapper = this.createSchemaMapper(sourceDBInfo, outOrientGraphUri, chosenMapper, xmlPath, nameResolver, handler,
                includedTables, excludedTables, migrationConfig, configurationHandler);

        Date globalEnd = new Date();

        OTeleporterContext.getInstance().getOutputManager().info("\n\nGraph model building complete in %s\n", OFunctionsHandler.getHMSFormat(globalStart, globalEnd));
        OTeleporterContext.getInstance().getOutputManager().info(OTeleporterContext.getInstance().getStatistics().toString());

        // Building Graph Model mapping (for graph rendering too)
        OConfiguration configuredGraph = configurationHandler.buildConfigurationFromMapper(this.mapper);
        ODocument configuredGraphDoc = configurationHandler.buildJSONDocFromConfiguration(configuredGraph);

        return configuredGraphDoc;
    }

    public abstract OER2GraphMapper createSchemaMapper(OSourceDatabaseInfo sourceDBInfo, String outOrientGraphUri, String chosenMapper,
                                                       String xmlPath, ONameResolver nameResolver, ODBMSDataTypeHandler handler, List<String> includedTables, List<String> excludedTables,
                                                       ODocument migrationConfig, OConfigurationHandler configHandler);

    protected abstract OConfigurationHandler buildConfigurationHandler();

}
