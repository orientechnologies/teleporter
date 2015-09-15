/*
 * Copyright 2015 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.teleporter.factory;

import java.util.List;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.mapper.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.OHibernate2GraphMapper;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;

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
