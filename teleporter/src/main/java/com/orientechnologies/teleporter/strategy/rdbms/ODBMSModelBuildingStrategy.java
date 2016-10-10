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
import com.orientechnologies.teleporter.util.OJSONConfigurationManager;

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
                                List<String> includedTables, List<String> excludedTables, String migrationConfigPath, OTeleporterContext context) {

        OSourceDatabaseInfo sourceDBInfo = (OSourceDatabaseInfo) sourceInfo;
        Date globalStart = new Date();

        ODataTypeHandlerFactory dataTypeHandlerFactory = new ODataTypeHandlerFactory();
        ODBMSDataTypeHandler handler = (ODBMSDataTypeHandler) dataTypeHandlerFactory.buildDataTypeHandler(sourceDBInfo.getDriverName(), context);
        OConfigurationHandler configurationHandler = this.buildConfigurationHandler();

        /*
         * Step 1,2
         */
        ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
        ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention, context);
        context.getStatistics().runningStepNumber = -1;

        // manage conf if present: loading
        OJSONConfigurationManager confManager = new OJSONConfigurationManager();
        ODocument config = confManager.loadMigrationConfig(outOrientGraphUri, migrationConfigPath, context);

        this.mapper = this.createSchemaMapper(sourceDBInfo, outOrientGraphUri, chosenMapper, xmlPath, nameResolver, handler,
                includedTables, excludedTables, config, configurationHandler, context);

        Date globalEnd = new Date();

        context.getOutputManager().info("\n\nGraph model building complete in %s\n", OFunctionsHandler.getHMSFormat(globalStart, globalEnd));
        context.getOutputManager().info(context.getStatistics().toString());

        // Graph Model translation
        OConfiguration configuredGraph = configurationHandler.buildConfigurationFromGraphModel(this.mapper, context);
        ODocument configuredGraphDoc = configurationHandler.buildJSONDocFromConfiguration(configuredGraph, context);

        return configuredGraphDoc;
    }

    public abstract OER2GraphMapper createSchemaMapper(OSourceDatabaseInfo sourceDBInfo, String outOrientGraphUri, String chosenMapper,
                                                       String xmlPath, ONameResolver nameResolver, ODBMSDataTypeHandler handler, List<String> includedTables, List<String> excludedTables,
                                                       ODocument migrationConfig, OConfigurationHandler configHandler, OTeleporterContext context);

    protected abstract OConfigurationHandler buildConfigurationHandler();

}
