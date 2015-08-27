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

import com.orientechnologies.plugin.teleporter.context.OTeleporterContext;
import com.orientechnologies.plugin.teleporter.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.plugin.teleporter.persistence.handler.OGenericDataTypeHandler;
import com.orientechnologies.plugin.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.plugin.teleporter.persistence.handler.OMySQLDataTypeHandler;
import com.orientechnologies.plugin.teleporter.persistence.handler.OOracleDataTypeHandler;
import com.orientechnologies.plugin.teleporter.persistence.handler.OPostgreSQLDataTypeHandler;

/**
 * Factory used to instantiate a specific DataTypeHandler according to the driver of the
 * DBMS from which the import is performed.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODataTypeHandlerFactory {

  public ODriverDataTypeHandler buildDataTypeHandler(String driver, OTeleporterContext context) {
    ODriverDataTypeHandler handler = null;

    switch(driver) {

    case "org.postgresql.Driver":   handler = new OPostgreSQLDataTypeHandler();
    break;

    case "com.mysql.jdbc.Driver":   handler = new OMySQLDataTypeHandler();
    break;

    case "oracle.jdbc.driver.OracleDriver": handler = new OOracleDataTypeHandler();
    break;
    
    case "org.hsqldb.jdbc.JDBCDriver": handler = new OHSQLDBDataTypeHandler();
    break;

    default :  handler = new OGenericDataTypeHandler();
    context.getStatistics().warningMessages.add("Driver " + driver + " is not supported. Thus problems may occur during type conversion.");
    break;
    }
    
    context.setDataTypeHandler(handler);

    return handler;
  }

}
