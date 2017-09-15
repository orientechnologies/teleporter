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

import com.orientechnologies.orient.output.OOutputStreamManager;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
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

public class ODBMSSimpleModelBuildingStrategy extends OAbstractDBMSModelBuildingStrategy {

  public ODBMSSimpleModelBuildingStrategy() {
  }

  @Override
  protected OConfigurationHandler buildConfigurationHandler() {
    return new OConfigurationHandler(false);
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
    OTeleporterContext.getInstance().getMessageHandler().info(this, "\n");
    if(OTeleporterContext.getInstance().getMessageHandler().getOutputManagerLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getMessageHandler().debug(this, "\n%s\n", ((OER2GraphMapper) mapper).getDataBaseSchema().toString());
    }

    // Step 2: Graph model building
    mapper.buildGraphModel(nameResolver);
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getMessageHandler().info(this, "\n");
    if(OTeleporterContext.getInstance().getMessageHandler().getOutputManagerLevel() == OOutputStreamManager.DEBUG_LEVEL) {
      OTeleporterContext.getInstance().getMessageHandler().debug(this, "\n%s\n", mapper.getGraphModel().toString());
    }

    // Step 3: eventual migrationConfigDoc applying
    mapper.applyImportConfiguration();

    return mapper;
  }

}
