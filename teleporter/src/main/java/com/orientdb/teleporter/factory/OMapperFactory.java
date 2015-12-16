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

package com.orientdb.teleporter.factory;

import java.util.List;

import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.mapper.OSource2GraphMapper;
import com.orientdb.teleporter.mapper.neo4j.ONeo4jSchema2GraphMapper;
import com.orientdb.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientdb.teleporter.mapper.rdbms.OHibernate2GraphMapper;

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

    // choosing strategy for migration from neo4j
    if(driver.equalsIgnoreCase("neo4j")) {

      switch(chosenMapper) {

      case "neo4jMapper":   mapper = new ONeo4jSchema2GraphMapper();
      break;

      default :  mapper = new ONeo4jSchema2GraphMapper();
      }
    }

    // choosing strategy for migration from mongoDB
    else if (driver.equalsIgnoreCase("mongoDB")) {
      // DOES NOTHING: no mapper required
    }

    else {

      switch(chosenMapper) {

      case "basicDBMapper":   mapper = new OER2GraphMapper(driver, uri, username, password, includedTables, excludedTables);
      break;

      case "hibernate":   mapper = new OHibernate2GraphMapper(driver, uri, username, password, xmlPath, includedTables, excludedTables);
      break;

      default :  mapper = new OER2GraphMapper(driver, uri, username, password, includedTables, excludedTables);
      }
    }

    return mapper;
  }

}
