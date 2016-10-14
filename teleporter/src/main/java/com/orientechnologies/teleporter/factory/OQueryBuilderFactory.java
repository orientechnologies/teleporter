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

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.OCommonQueryBuilder;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.OMysqlQueryBuilder;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.OPostgreSQLQueryBuilder;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.OQueryBuilder;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OQueryBuilderFactory {

  public OQueryBuilder buildQueryBuilder(String driver) {
    OQueryBuilder queryBuilder;

    switch(driver) {

      case "oracle.jdbc.driver.OracleDriver": queryBuilder = new OCommonQueryBuilder();
        break;

      case "com.microsoft.sqlserver.jdbc.SQLServerDriver": queryBuilder = new OCommonQueryBuilder();
        break;

      case "com.mysql.jdbc.Driver":   queryBuilder = new OMysqlQueryBuilder();
        break;

      case "org.postgresql.Driver":   queryBuilder = new OPostgreSQLQueryBuilder();
        break;

      case "org.hsqldb.jdbc.JDBCDriver": queryBuilder = new OCommonQueryBuilder();
        break;

      default :  queryBuilder = new OCommonQueryBuilder();
        OTeleporterContext.getInstance().getStatistics().warningMessages.add("Driver " + driver + " is not completely supported, the common query builder will be adopted for the case-sensitive queries. "
                + "Thus problems may occur during the querying.");
        break;
    }

    return queryBuilder;
  }

}
