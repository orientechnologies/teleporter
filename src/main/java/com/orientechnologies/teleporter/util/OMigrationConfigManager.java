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

package com.orientechnologies.teleporter.util;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.output.OPluginMessageHandler;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OMigrationConfigManager {

  // config info
  private static final String configurationDirectoryName = "teleporter-config/";
  private static final String configFileName = "migration-config.json"; // path
  // ORIENTDB_HOME/<db-name>/teleporter-config/migration-config.json
  private static final String sourceInfoFileName = "sources-access-info.json"; // path
  // ORIENTDB_HOME/<db-name>/teleporter-config/sources-access-info.json
  private static boolean configPresentInDB;

  /**
   * Loading eventual migrationConfigDoc from an external configuration file. Look for the config in
   * the <db-path>/teleporter-config/ path: (i) - if the external config path is available then load
   * the config and use it for the steps 1,2,3 in the <db-path>/teleporter-config/ path
   * (migrationConfigDoc.json) (ii) - else execute strategy without migration config
   */
  public static ODocument loadMigrationConfigFromFile(String configurationPath) {
    return loadMigrationConfigFromFile(
        null, configurationPath, OTeleporterContext.getInstance().getMessageHandler());
  }

  /**
   * Loading eventual migrationConfigDoc from an external configuration file. Look for the config in
   * the <db-path>/teleporter-config/ path: (i) - if the external config path is available then load
   * the config and use it for the steps 1,2,3 in the <db-path>/teleporter-config/ path
   * (migrationConfigDoc.json) (ii) - else execute strategy without migration config
   */
  public static ODocument loadMigrationConfigFromFile(
      Object requester, String configurationPath, OPluginMessageHandler messageHandler) {

    ODocument config = null;
    try {
      config = OFileManager.buildJsonFromFile(configurationPath);
      // (i)
      if (config != null) {
        messageHandler.info(
            OMigrationConfigManager.class,
            "Configuration correctly loaded from %s.\n",
            configurationPath);
      }
    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.printExceptionMessage(requester, messageHandler, e, mess, "error");
      OTeleporterContext.printExceptionStackTrace(requester, messageHandler, e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    return config;
  }

  public static String buildConfigurationFilePath(String outOrientGraphUri, String configFileName) {
    if (outOrientGraphUri.contains("\\")) {
      outOrientGraphUri = outOrientGraphUri.replace("\\", "/");
    }

    // checking the presence of the migrationConfigDoc in the target db
    if (!(outOrientGraphUri.charAt(outOrientGraphUri.length() - 1) == '/')) {
      outOrientGraphUri += "/";
    }
    String outDBConfigPath = outOrientGraphUri + configurationDirectoryName + configFileName;
    if (outDBConfigPath.contains("plocal:")) {
      outDBConfigPath = outDBConfigPath.replace("plocal:", "");
    } else if (outDBConfigPath.contains("embedded:")) {
      outDBConfigPath = outDBConfigPath.replace("embedded:", "");
    }
    return outDBConfigPath;
  }

  /*public static void copyConfigurationInDatabase(ODocument config, String configurationPath, String outDBConfigPath) {

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
  }*/

  public static void writeConfigurationInTargetDB(
      ODocument migrationConfig, String outOrientGraphUri) {

    String outDBConfigPath = prepareConfigDirectoryForWriting(outOrientGraphUri);
    String jsonSourcesInfo = migrationConfig.toJSON("prettyPrint");
    try {
      OFileManager.writeFileFromText(jsonSourcesInfo, outDBConfigPath, false);
    } catch (IOException e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
    }
  }

  public static void writeConfigurationInTargetDB(String migrationConfig, String outOrientGraphUri)
      throws IOException {

    String outDBConfigPath = prepareConfigDirectoryForWriting(outOrientGraphUri);
    OFileManager.writeFileFromText(migrationConfig, outDBConfigPath, false);
  }

  private static String prepareConfigDirectoryForWriting(String outOrientGraphUri) {

    String outDBConfigPath = buildConfigurationFilePath(outOrientGraphUri, configFileName);
    File confFileInOrientDB = new File(outDBConfigPath);

    if (confFileInOrientDB.exists()) {
      confFileInOrientDB.delete();
    }
    return outDBConfigPath;
  }

  /**
   * Loading eventual sourceAccessInfoDoc. Look for the config in the <db-path>/teleporter-config/
   * path: (i) - if db and sourceAccessInfoDoc are present use the default config (ii) - else
   * execute strategy without sourceAccessInfoConfigDoc
   */
  public static ODocument loadSourceInfo(String outOrientGraphUri) {

    String outDBConfigPath = buildConfigurationFilePath(outOrientGraphUri, sourceInfoFileName);
    File confFileInOrientDB = new File(outDBConfigPath);

    if (confFileInOrientDB.exists()) configPresentInDB = true;
    else configPresentInDB = false;

    // (i)
    ODocument sourcesAccessInfo = null;
    try {
      if (configPresentInDB) {
        sourcesAccessInfo = OFileManager.buildJsonFromFile(outDBConfigPath);
        OTeleporterContext.getInstance()
            .getMessageHandler()
            .info(
                OMigrationConfigManager.class,
                "Sources' access info correctly loaded from %s.\n",
                outDBConfigPath);
      } else {
        // (iii)
        OTeleporterContext.getInstance()
            .getMessageHandler()
            .info(OMigrationConfigManager.class, "No sources' access info file was found.\n");
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
    for (ODocument currSourceInfo : sources) {
      String[] fieldNames = currSourceInfo.fieldNames();
      String sourceIdName = fieldNames[0];
      ODocument info = currSourceInfo.field(sourceIdName);
      String driverName = info.field("driverName");
      String url = info.field("url");
      String user = info.field("username");
      String passwd = info.field("password");
      List<String> primaryKey = info.field("primaryKey");
      OSourceDatabaseInfo sourceInfo =
          new OSourceDatabaseInfo(sourceIdName, driverName, url, user, passwd, primaryKey);
      sourcesInfo.add(sourceInfo);
    }
    return sourcesInfo;
  }

  public static void upsertSourceDatabaseInfo(
      List<OSourceDatabaseInfo> sourcesInfo, String outOrientGraphUri) {

    String outDBConfigPath = buildConfigurationFilePath(outOrientGraphUri, sourceInfoFileName);
    File confFileInOrientDB = new File(outDBConfigPath);

    if (confFileInOrientDB.exists()) {
      confFileInOrientDB.delete();
    }

    // write the updated source info config
    ODocument sourcesInfoDoc = new ODocument();
    List<ODocument> sources = new LinkedList<ODocument>();

    for (OSourceDatabaseInfo currSourceInfo : sourcesInfo) {
      ODocument source = new ODocument();
      ODocument sourceInfo = new ODocument();
      sourceInfo.field("driverName", currSourceInfo.getDriverName());
      sourceInfo.field("url", currSourceInfo.getUrl());
      sourceInfo.field("username", currSourceInfo.getUsername());
      sourceInfo.field("password", currSourceInfo.getPassword());
      source.field(currSourceInfo.getSourceIdName(), sourceInfo);
      sources.add(source);
    }
    sourcesInfoDoc.field("sources", sources);

    String jsonSourcesInfo = sourcesInfoDoc.toJSON("prettyPrint");
    try {
      OFileManager.writeFileFromText(jsonSourcesInfo, outDBConfigPath, false);
    } catch (IOException e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
    }
  }

  public static String getConfigurationDirectoryName() {
    return configurationDirectoryName;
  }

  public static String getConfigFileName() {
    return configFileName;
  }

  public static String getSourceInfoFileName() {
    return sourceInfoFileName;
  }
}
