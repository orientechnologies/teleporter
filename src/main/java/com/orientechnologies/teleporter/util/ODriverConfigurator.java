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

package com.orientechnologies.teleporter.util;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.persistence.util.ODBSourceConnection;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * Executes an automatic migrationConfigDoc of the chosen driver JDBC.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class ODriverConfigurator {

  public static final String DRIVERS         = "http://orientdb.com/jdbc-drivers.json";
  private final       String localJsonPath   = "../config/jdbc-drivers.json";
  private final       String driverClassPath = "../lib/";
  private ODocument                 driverInfo;
  private Map<String, List<String>> driver2filesIdentifier;

  public ODriverConfigurator() {
    this.driver2filesIdentifier = new LinkedHashMap<String, List<String>>();
    this.fillMap();
  }

  /**
   *
   */
  private void fillMap() {

    // Oracle identifiers
    List<String> oracleIdentifiers = new LinkedList<String>();
    oracleIdentifiers.add("ojdbc");
    driver2filesIdentifier.put("oracle", oracleIdentifiers);

    // SQLServer identifiers
    List<String> sqlserverIdentifiers = new LinkedList<String>();
    sqlserverIdentifiers.add("sqljdbc");
    sqlserverIdentifiers.add("jtds");
    driver2filesIdentifier.put("sqlserver", sqlserverIdentifiers);

    // MySQL identifiers
    List<String> mysqlIdentifiers = new LinkedList<String>();
    mysqlIdentifiers.add("mysql");
    driver2filesIdentifier.put("mysql", mysqlIdentifiers);

    // PostgreSQL identifiers
    List<String> postgresqlIdentifiers = new LinkedList<String>();
    postgresqlIdentifiers.add("postgresql");
    driver2filesIdentifier.put("postgresql", postgresqlIdentifiers);

    // HyperSQL identifiers
    List<String> hypersqlIdentifiers = new LinkedList<String>();
    hypersqlIdentifiers.add("hsqldb");
    driver2filesIdentifier.put("hypersql", hypersqlIdentifiers);

  }

  /**
   * It performs a fetching of the driver class name from the driver name (corresponding to the chosen DBMS)
   * Connection to the 'http://orientdb.com/jdbc-drivers.json' resource if needed.
   *
   * @param driverName (case insensitive)
   *
   * @return driverClassName
   */
  public String fetchDriverClassName(String driverName) {

    String driverClassName = null;
    driverName = driverName.toLowerCase(Locale.ENGLISH);

    try {

      if (this.driverInfo == null) {
        // fetching online JSON
        this.driverInfo = readJsonFromRemoteUrl(DRIVERS);
      }

      ODocument fields = null;

      // recovering driver class name
      if (driverName.equals("oracle")) {
        fields = this.driverInfo.field("Oracle");
      }
      if (driverName.equals("sqlserver")) {
        fields = this.driverInfo.field("SQLServer");
      } else if (driverName.equals("mysql")) {
        fields = this.driverInfo.field("MySQL");
      } else if (driverName.equals("postgresql")) {
        fields = this.driverInfo.field("PostgreSQL");
      } else if (driverName.equals("hypersql")) {
        fields = this.driverInfo.field("HyperSQL");
      }
      driverClassName = (String) fields.field("className");
    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    return driverClassName;
  }

  /**
   * It Checks if the requested driver is already present in the classpath, if not present it downloads the last available driver
   * version.
   * Connection to the 'http://orientdb.com/jdbc-drivers.json' resource if needed.
   *
   * @param driverName (case insensitive)
   */

  public void checkDriverConfiguration(String driverName) {

    this.checkDriverConfiguration(driverName, this.driverClassPath);
  }

  /**
   * @param driverName
   * @param driverClassPath
   */

  public void checkDriverConfiguration(String driverName, String driverClassPath) {

    driverName = driverName.toLowerCase(Locale.ENGLISH);

    try {

      if (this.driverInfo == null) {
        // fetching online JSON
        this.driverInfo = readJsonFromRemoteUrl(DRIVERS);
      }

      ODocument fields = null;

      // recovering driver class name
      if (driverName.equals("oracle")) {
        fields = this.driverInfo.field("Oracle");
      }
      if (driverName.equals("sqlserver")) {
        fields = this.driverInfo.field("SQLServer");
      } else if (driverName.equals("mysql")) {
        fields = this.driverInfo.field("MySQL");
      } else if (driverName.equals("postgresql")) {
        fields = this.driverInfo.field("PostgreSQL");
      } else if (driverName.equals("hypersql")) {
        fields = this.driverInfo.field("HyperSQL");
      }

      // if the driver is not present, it will be downloaded
      String driverPath = isDriverAlreadyPresent(driverName, driverClassPath);

      if (driverPath == null) {

        OTeleporterContext.getInstance().getMessageHandler()
            .info(this, "\nDownloading the necessary JDBC driver in ORIENTDB_HOME/lib ...\n");

        // download last available jdbc driver version
        String driverDownldUrl = (String) fields.field("url");
        URL website = new URL(driverDownldUrl);
        String fileName = driverDownldUrl.substring(driverDownldUrl.lastIndexOf('/') + 1, driverDownldUrl.length());
        ReadableByteChannel rbc = Channels.newChannel(website.openStream());
        @SuppressWarnings("resource")
        FileOutputStream fos = new FileOutputStream(driverClassPath + fileName);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        fos.close();

        driverPath = driverClassPath + fileName;

        if (driverName.equalsIgnoreCase("SQLServer")) {
          OFileManager.extractAll(driverPath, driverClassPath);
          try {
            OFileManager.deleteResource(driverPath);
          } catch (IOException e) {
            OTeleporterContext.getInstance().getMessageHandler()
                .info(this, "The %s package file was not correctly deleted from the %s path.", driverPath, driverClassPath);
          }
          String[] split = driverPath.split(".jar");
          driverPath = split[0] + ".jar";
        }

        OTeleporterContext.getInstance().getMessageHandler().info(this, "Driver JDBC downloaded.\n");
      }

      // saving driver
      OTeleporterContext.getInstance().setDriverDependencyPath(driverPath);

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

  }

  /**
   * @param driverName
   * @param classPath
   *
   * @return the path of the driver
   */
  private String isDriverAlreadyPresent(String driverName, String classPath) {

    File dir = new File(classPath);
    File[] files = dir.listFiles();

    if (files == null) {
      // ../lib does not exist yet, so create it.
      dir.mkdirs();
      files = dir.listFiles();
    }

    for (String identifier : this.driver2filesIdentifier.get(driverName)) {

      for (int i = 0; i < files.length; i++) {
        if (files[i].getName().startsWith(identifier))
          return files[i].getPath();
      }
    }

    // the driver is not present, thus it's returned null as path
    return null;
  }

  /**
   * It reads the driver config from remote url and saves it in local. Then it reads the local file to build the ODocument returned by the method.
   * If the driver was not downloaded in local, last config version is considered. If there is not any local configuration (first execution forms scratch)
   * an exception is thrown.
   *
   * @param url
   * @return config
   */
  public ODocument readJsonFromRemoteUrl(String url) {

    InputStream is = null;
    ODocument json = null;

    try {

      URL urlObj = new URL(url);
      URLConnection urlConn = urlObj.openConnection();
      urlConn.setRequestProperty("User-Agent", "Teleporter");
      boolean downloadedNewJsonDrivers = true;

      try {
        is = urlConn.getInputStream();
      } catch (IOException e1) {
        downloadedNewJsonDrivers = false;
        try {
          // read json from the file in the ORIENTDB_HOME/config path
          is = new FileInputStream(new File(this.localJsonPath));
        } catch (IOException e2) {
          String mess = "The jdbc-drivers json cannot be found. The connection to http://orientdb.com/jdbc-drivers.json did not succeed, and the \"jdbc-drivers.json\" file is not present in ORIENTDB_HOME/config neither.\n";
          OTeleporterContext.getInstance().printExceptionMessage(e2, mess, "error");
          OTeleporterContext.getInstance().printExceptionStackTrace(e2, "error");
          throw new OTeleporterRuntimeException(e2);
        }
      }

      json = new ODocument();

      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
      String jsonText = OFileManager.readAllTextFile(rd);
      json.fromJSON(jsonText, "noMap");

      // writing the just downloaded json into /config
      if (downloadedNewJsonDrivers) {
        OFileManager.writeFileFromText(jsonText, this.localJsonPath, false);
      }

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    } finally {
      try {
        is.close();
      } catch (Exception e) {
        String mess = "";
        OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
        OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
        throw new OTeleporterRuntimeException(e);
      }
    }
    return json;
  }

  /**
   * Checks connection to a source database identified through the 4 access parameters.
   * The driver configuration is managed in the ../lib directory.
   *
   * @param driver
   * @param uri
   * @param username
   * @param password
   *
   * @throws Exception
   */
  public void checkConnection(String driver, String uri, String username, String password) throws Exception {

    this.checkDriverConfiguration(driver);
    this.checkConnectionSubRoutine(driver, uri, username, password);
  }

  /**
   * Checks connection to a source database identified through the 4 access parameters.
   * The driver configuration is managed in the directory specified as last argument.
   *
   * @param driver
   * @param uri
   * @param username
   * @param password
   * @param driverClassPath
   *
   * @throws Exception
   */
  public void checkConnection(String driver, String uri, String username, String password, String driverClassPath)
      throws Exception {

    this.checkDriverConfiguration(driver, driverClassPath);
    this.checkConnectionSubRoutine(driver, uri, username, password);
  }

  /**
   * Once the driver configuration is complete, it checks the connection to the source database.
   *
   * @param driver
   * @param uri
   * @param username
   * @param password
   *
   * @throws SQLException
   */
  private void checkConnectionSubRoutine(String driver, String uri, String username, String password) throws SQLException {

    String driverName = this.fetchDriverClassName(driver);
    Connection connection = null;
    try {
      connection = ODBSourceConnection.getConnection(driverName, uri, username, password);
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  /**
   * It gets the connection to the source database.
   *
   * @param driver
   * @param uri
   * @param username
   * @param password
   *
   * @throws SQLException
   */
  public Connection getDBMSConnection(String driver, String uri, String username, String password) throws SQLException {

    String driverName = this.fetchDriverClassName(driver);
    Connection connection = null;
    try {
      connection = ODBSourceConnection.getConnection(driverName, uri, username, password);
    } catch(Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }
    return connection;
  }

}