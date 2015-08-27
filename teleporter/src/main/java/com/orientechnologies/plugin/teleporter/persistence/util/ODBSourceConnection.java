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

package com.orientechnologies.plugin.teleporter.persistence.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;

import com.orientechnologies.plugin.teleporter.context.OTeleporterContext;

/**
 * Utility class to which connection with source DB is delegated.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ODBSourceConnection {

  private String driver;
  private String uri;
  private String username;
  private String password;


  public ODBSourceConnection(String driver, String uri, String username, String password) {	
    this.driver = driver;
    this.uri = uri;
    this.username = username;
    this.password = password;

  }

  public Connection getConnection(OTeleporterContext context) {

    Connection connection = null;
    try {
      //      Class.forName(driver);
      //      connection = DriverManager.getConnection(uri,username, password);

      URL u = new URL("jar:file:./" + context.getDriverDependencyPath() + "!/");
      URLClassLoader ucl = new URLClassLoader(new URL[] { u });
      Driver d = (Driver) Class.forName(this.driver, true, ucl).newInstance();
      DriverManager.registerDriver(new ODriverShim(d));
      connection = DriverManager.getConnection(uri, username, password);

    } catch(Exception e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
      System.exit(0);
    }
    return connection;

  }

  public String getDriver() {
    return driver;
  }

  public String getUri() {
    return uri;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}