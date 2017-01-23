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

package com.orientechnologies.teleporter.factory;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.OHibernate2GraphMapper;
import com.orientechnologies.teleporter.model.OSourceInfo;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;

import java.util.List;

/**
 * Factory used to instantiate the chosen 'Mapper' which will be adopted for the source schema building.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OMapperFactory {

  public OMapperFactory() {
  }

  public OSource2GraphMapper buildMapper(String chosenMapper, OSourceInfo sourceInfo, String xmlPath, List<String> includedTables,
      List<String> excludedTables, OConfiguration configuration) {

    OSource2GraphMapper mapper = null;

    switch (chosenMapper) {

    case "basicDBMapper":
      mapper = new OER2GraphMapper((OSourceDatabaseInfo) sourceInfo, includedTables, excludedTables, configuration);
      break;

    case "hibernate":
      mapper = new OHibernate2GraphMapper((OSourceDatabaseInfo) sourceInfo, xmlPath, includedTables, excludedTables, configuration);
      break;

    default:
      mapper = new OER2GraphMapper((OSourceDatabaseInfo) sourceInfo, includedTables, excludedTables, configuration);
    }

    return mapper;
  }

}