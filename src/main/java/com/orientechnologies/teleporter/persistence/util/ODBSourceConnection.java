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

package com.orientechnologies.teleporter.persistence.util;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;

/**
 * Utility class to which connection with source DB is delegated.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class ODBSourceConnection {

  public static Connection getConnection(OSourceDatabaseInfo sourceDBInfo) {

    Connection connection = null;
    String driver = sourceDBInfo.getDriverName();
    String uri = sourceDBInfo.getUrl();
    String username = sourceDBInfo.getUsername();
    String password = sourceDBInfo.getPassword();

    try {
      URL u =
          new URL("jar:file:" + OTeleporterContext.getInstance().getDriverDependencyPath() + "!/");
      URLClassLoader ucl = new URLClassLoader(new URL[] {u});
      Driver d = (Driver) Class.forName(driver, true, ucl).newInstance();
      DriverManager.registerDriver(new ODriverShim(d));
      connection = DriverManager.getConnection(uri, username, password);

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }
    return connection;
  }

  public static Connection getConnection(
      String driver, String uri, String username, String password) {

    Connection connection = null;

    try {
      URL u =
          new URL("jar:file:" + OTeleporterContext.getInstance().getDriverDependencyPath() + "!/");
      URLClassLoader ucl = new URLClassLoader(new URL[] {u});
      Driver d = (Driver) Class.forName(driver, true, ucl).newInstance();
      DriverManager.registerDriver(new ODriverShim(d));
      connection = DriverManager.getConnection(uri, username, password);

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }
    return connection;
  }
}
