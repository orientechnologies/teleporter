/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.plugin.teleporter.factory;

import java.util.List;

import com.orientechnologies.plugin.teleporter.context.OTeleporterContext;
import com.orientechnologies.plugin.teleporter.mapper.OER2GraphMapper;
import com.orientechnologies.plugin.teleporter.mapper.OHibernate2GraphMapper;
import com.orientechnologies.plugin.teleporter.mapper.OSource2GraphMapper;

/**
 * Factory used to instantiate the chosen 'Mapper' which will be adopted for the source schema building.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OMapperFactory {
  
  public OMapperFactory() {}

  public OSource2GraphMapper buildMapper(String chosenMapper, String driver, String uri, String username, String password, String xmlPath, List<String> includedTables, List<String> excludedTables, OTeleporterContext context) {
    
    OSource2GraphMapper mapper = null;

    switch(chosenMapper) {
    
    case "basicDBMapper":   mapper = new OER2GraphMapper(driver, uri, username, password, includedTables, excludedTables);
    break;

    case "hibernate":   mapper = new OHibernate2GraphMapper(driver, uri, username, password, xmlPath, includedTables, excludedTables);
    break;

    default :  mapper = new OER2GraphMapper(driver, uri, username, password, includedTables, excludedTables);
    }

    return mapper;
  }

}
