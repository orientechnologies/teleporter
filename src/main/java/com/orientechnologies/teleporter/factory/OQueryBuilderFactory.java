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

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.OCommonQueryBuilder;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.OMysqlQueryBuilder;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.OPostgreSQLQueryBuilder;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.OQueryBuilder;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OQueryBuilderFactory {

  public OQueryBuilder buildQueryBuilder(String driver) {
    OQueryBuilder queryBuilder;

    switch (driver) {

    case "oracle.jdbc.driver.OracleDriver":
      queryBuilder = new OCommonQueryBuilder();
      break;

    case "com.microsoft.sqlserver.jdbc.SQLServerDriver":
      queryBuilder = new OCommonQueryBuilder();
      break;

    case "com.mysql.jdbc.Driver":
      queryBuilder = new OMysqlQueryBuilder();
      break;

    case "org.postgresql.Driver":
      queryBuilder = new OPostgreSQLQueryBuilder();
      break;

    case "org.hsqldb.jdbc.JDBCDriver":
      queryBuilder = new OCommonQueryBuilder();
      break;

    default:
      queryBuilder = new OCommonQueryBuilder();
      OTeleporterContext.getInstance().getStatistics().warningMessages.add("Driver " + driver
          + " is not completely supported, the common query builder will be adopted for the case-sensitive queries. "
          + "Thus problems may occur during the querying.");
      break;
    }

    return queryBuilder;
  }

}
