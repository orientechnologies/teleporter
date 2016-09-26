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
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ODBSourceConnection {


  public static Connection getConnection(OSourceDatabaseInfo sourceDBInfo, OTeleporterContext context) {

    Connection connection = null;
    String driver = sourceDBInfo.getDriverName();
    String uri = sourceDBInfo.getUrl();
    String username =  sourceDBInfo.getUsername();
    String password = sourceDBInfo.getPassword();

    try {
      URL u = new URL("jar:file:" + context.getDriverDependencyPath() + "!/");
      URLClassLoader ucl = new URLClassLoader(new URL[] { u });
      Driver d = (Driver) Class.forName(driver, true, ucl).newInstance();
      DriverManager.registerDriver(new ODriverShim(d));
      connection = DriverManager.getConnection(uri, username, password);

    } catch (Exception e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }
    return connection;
  }

  public static Connection getConnection(String driver, String uri, String username, String password, OTeleporterContext context) {

    Connection connection = null;

    try {
      URL u = new URL("jar:file:" + context.getDriverDependencyPath() + "!/");
      URLClassLoader ucl = new URLClassLoader(new URL[] { u });
      Driver d = (Driver) Class.forName(driver, true, ucl).newInstance();
      DriverManager.registerDriver(new ODriverShim(d));
      connection = DriverManager.getConnection(uri, username, password);

    } catch (Exception e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }
    return connection;
  }


}