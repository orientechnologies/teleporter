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

package com.orientechnologies.teleporter.factory;

import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.OHibernate2GraphMapper;
import com.orientechnologies.teleporter.model.OSourceInfo;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import java.util.List;

/**
 * Factory used to instantiate the chosen 'Mapper' which will be adopted for the source schema
 * building.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OMapperFactory {

  public OMapperFactory() {}

  public OSource2GraphMapper buildMapper(
      String chosenMapper,
      OSourceInfo sourceInfo,
      String xmlPath,
      List<String> includedTables,
      List<String> excludedTables,
      OConfiguration configuration) {

    OSource2GraphMapper mapper = null;

    switch (chosenMapper) {
      case "basicDBMapper":
        mapper =
            new OER2GraphMapper(
                (OSourceDatabaseInfo) sourceInfo, includedTables, excludedTables, configuration);
        break;

      case "hibernate":
        mapper =
            new OHibernate2GraphMapper(
                (OSourceDatabaseInfo) sourceInfo,
                xmlPath,
                includedTables,
                excludedTables,
                configuration);
        break;

      default:
        mapper =
            new OER2GraphMapper(
                (OSourceDatabaseInfo) sourceInfo, includedTables, excludedTables, configuration);
    }

    return mapper;
  }
}
