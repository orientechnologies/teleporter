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
