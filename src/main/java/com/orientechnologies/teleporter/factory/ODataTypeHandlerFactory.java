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
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.persistence.handler.OMySQLDataTypeHandler;
import com.orientechnologies.teleporter.persistence.handler.OOracleDataTypeHandler;
import com.orientechnologies.teleporter.persistence.handler.OPostgreSQLDataTypeHandler;
import com.orientechnologies.teleporter.persistence.handler.OSQLServerDataTypeHandler;

/**
 * Factory used to instantiate a specific DataTypeHandler according to the driver of the
 * DBMS from which the import is performed.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class ODataTypeHandlerFactory {

  public ODriverDataTypeHandler buildDataTypeHandler(String driver) {
    ODriverDataTypeHandler handler = null;

    switch (driver) {

    case "oracle.jdbc.driver.OracleDriver":
      handler = new OOracleDataTypeHandler();
      break;

    case "com.microsoft.sqlserver.jdbc.SQLServerDriver":
      handler = new OSQLServerDataTypeHandler();
      break;

    case "com.mysql.jdbc.Driver":
      handler = new OMySQLDataTypeHandler();
      break;

    case "org.postgresql.Driver":
      handler = new OPostgreSQLDataTypeHandler();
      break;

    case "org.hsqldb.jdbc.JDBCDriver":
      handler = new OHSQLDBDataTypeHandler();
      break;

    default:
      handler = new ODBMSDataTypeHandler();
      OTeleporterContext.getInstance().getStatistics().warningMessages
          .add("Driver " + driver + " is not completely supported. Thus problems may occur during type conversion.");
      break;
    }

    OTeleporterContext.getInstance().setDataTypeHandler(handler);

    return handler;
  }

}
