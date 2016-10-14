/*
 * Copyright 2016 OrientDB LTD (info--at--orientdb.com)
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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.main.OTeleporter;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OMigrationConfigManager {


  // config info
  private final static String configurationDirectoryName = "teleporter-config/";
  private final static String configFileName = "migration-config.json";
  private final static String sourceInfoFileName = "sources-access-info.json";
  private static String outDBConfigPath;       // path ORIENTDB_HOME/<db-name>/teleporter-config/migartion-config.json
  private static String outDBSourceInfoPath;       // path ORIENTDB_HOME/<db-name>/teleporter-config/sources-access-info.json
  private static boolean configPresentInDB;


  /**
   * Loading eventual migrationConfigDoc.
   * Look for the config in the <db-path>/teleporter-config/ path:
   *  (i) - if db and migrationConfigDoc are present use the default config
   *      - else
   *       (ii)  - if an external config path was passed as argument then load the config, use it for the steps 1,2,3 and then copy it
   *               in the <db-path>/teleporter-config/ path (migrationConfigDoc.json)
   *       (iii) - else execute strategy without migrationConfigDoc
   **/
  public static ODocument loadMigrationConfig(String outOrientGraphUri, String configurationPath) {

    if(outOrientGraphUri.contains("\\")) {
      outOrientGraphUri = outOrientGraphUri.replace("\\","/");
    }

    // checking the presence of the migrationConfigDoc in the target db
    if (!(outOrientGraphUri.charAt(outOrientGraphUri.length() - 1) == '/')) {
      outOrientGraphUri += "/";
    }
    outDBConfigPath = outOrientGraphUri + configurationDirectoryName + configFileName;
    outDBConfigPath = outDBConfigPath.replace("plocal:", "");
    File confFileInOrientDB = new File(outDBConfigPath);

    if (confFileInOrientDB.exists())
      configPresentInDB = true;
    else
      configPresentInDB = false;

    // (i)
    ODocument config = null;
    try {
      if (configPresentInDB) {
        config = OFileManager.buildJsonFromFile(outDBConfigPath);
        OTeleporterContext.getInstance().getOutputManager().info("Configuration correctly loaded from %s.\n", outDBConfigPath);
      } else {
        config = OFileManager.buildJsonFromFile(configurationPath);
        // (ii)
        if (config != null) {
          OTeleporterContext.getInstance().getOutputManager().info("Configuration correctly loaded from %s and saved in %s.\n", configurationPath, outDBConfigPath);

          // manage conf if present: updating in the db directory
          copyConfigurationInDatabase(config, configurationPath);
        }
        // (iii)
        else {
          OTeleporterContext.getInstance().getOutputManager().info("No migrationConfigDoc file was found. Migration will be performed with standard mapping policies.\n");
        }
      }
    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    return config;
  }

  public static void copyConfigurationInDatabase(ODocument config, String configurationPath) {

    // if we have config in input and it is not present in the DB then we copy it in the <db-path>/teleporter-config/ path
    if (config != null) {
      if (!configPresentInDB) {
        try {
          copyConfigIntoTargetDB(configurationPath, outDBConfigPath);
        } catch (IOException e) {
          String mess = "";
          OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
          OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
          throw new OTeleporterRuntimeException(e);
        }
      }
    }
  }

  private static void copyConfigIntoTargetDB(String sourceConfigPath, String destinationConfigPath) throws IOException {

    File sourceConfig = new File(sourceConfigPath);
    File destinationConfig = new File(destinationConfigPath);
    destinationConfig.getParentFile().mkdirs();
    destinationConfig.createNewFile();
    FileChannel in = new FileInputStream(sourceConfig).getChannel();
    FileChannel out = new FileOutputStream(destinationConfig).getChannel();
    out.transferFrom(in, 0, in.size());
  }

  /**
   * Loading eventual sourceAccessInfoDoc.
   * Look for the config in the <db-path>/teleporter-config/ path:
   *  (i) - if db and sourceAccessInfoDoc are present use the default config
   * (ii) - else execute strategy without sourceAccessInfoConfigDoc
   **/
  public static ODocument loadSourceInfo(String outOrientGraphUri) {

    if(outOrientGraphUri.contains("\\")) {
      outOrientGraphUri = outOrientGraphUri.replace("\\","/");
    }

    // checking the presence of the migrationConfigDoc in the target db
    if (!(outOrientGraphUri.charAt(outOrientGraphUri.length() - 1) == '/')) {
      outOrientGraphUri += "/";
    }
    outDBConfigPath = outOrientGraphUri + configurationDirectoryName + sourceInfoFileName;
    outDBConfigPath = outDBConfigPath.replace("plocal:", "");
    File confFileInOrientDB = new File(outDBConfigPath);

    if (confFileInOrientDB.exists())
      configPresentInDB = true;
    else
      configPresentInDB = false;

    // (i)
    ODocument sourcesAccessInfo = null;
    try {
      if (configPresentInDB) {
        sourcesAccessInfo = OFileManager.buildJsonFromFile(outDBConfigPath);
        OTeleporterContext.getInstance().getOutputManager().info("Sources' access info correctly loaded from %s.\n", outDBConfigPath);
      } else {
        // (iii)
        OTeleporterContext.getInstance().getOutputManager().info("No sources' access info file was found.\n");
      }
    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    return sourcesAccessInfo;
  }

  public static List<OSourceDatabaseInfo> extractSourceDatabaseInfo(ODocument sourcesInfoDoc) {

    List<OSourceDatabaseInfo> sourcesInfo = new LinkedList<OSourceDatabaseInfo>();
    List<ODocument> sources = sourcesInfoDoc.field("sources");
    for(ODocument currSourceInfo: sources) {
      String[] fieldNames = currSourceInfo.fieldNames();
      String sourceIdName = fieldNames[0];
      ODocument info = currSourceInfo.field(sourceIdName);
      String driverName = info.field("driverName");
      String url = info.field("url");
      String user = info.field("username");
      String passwd = info.field("password");
      OSourceDatabaseInfo sourceInfo = new OSourceDatabaseInfo(sourceIdName, driverName, url, user, passwd);
      sourcesInfo.add(sourceInfo);
    }
    return sourcesInfo;
  }

  public static void upsertSourceDatabaseInfo(List<OSourceDatabaseInfo> sourcesInfo, String outOrientGraphUri) {

    if(outOrientGraphUri.contains("\\")) {
      outOrientGraphUri = outOrientGraphUri.replace("\\","/");
    }

    if (!(outOrientGraphUri.charAt(outOrientGraphUri.length() - 1) == '/')) {
      outOrientGraphUri += "/";
    }

    outDBSourceInfoPath = outOrientGraphUri + configurationDirectoryName + sourceInfoFileName;
    outDBSourceInfoPath = outDBSourceInfoPath.replace("plocal:", "");

    File confFileInOrientDB = new File(outDBSourceInfoPath);

    if (confFileInOrientDB.exists()) {
      confFileInOrientDB.delete();
    }

    // write the updated source info config
    ODocument sourcesInfoDoc = new ODocument();
    List<ODocument> sources = new LinkedList<ODocument>();

    for(OSourceDatabaseInfo currSourceInfo: sourcesInfo) {
      ODocument source = new ODocument();
      ODocument sourceInfo = new ODocument();
      sourceInfo.field("driverName",currSourceInfo.getDriverName());
      sourceInfo.field("url",currSourceInfo.getUrl());
      sourceInfo.field("username",currSourceInfo.getUsername());
      sourceInfo.field("password",currSourceInfo.getPassword());
      source.field(currSourceInfo.getSourceIdName(), sourceInfo);
      sources.add(source);
    }
    sourcesInfoDoc.field("sources",sources);

    String jsonSourcesInfo = sourcesInfoDoc.toJSON("prettyPrint");
    try {
      OFileManager.writeFileFromText(jsonSourcesInfo, outDBSourceInfoPath);
    }catch(IOException e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
    }

  }
}
