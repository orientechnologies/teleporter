/*
 * Copyright 2015 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.teleporter.main;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.exception.OTeleporterIOException;
import com.orientechnologies.teleporter.factory.OStrategyFactory;
import com.orientechnologies.teleporter.http.OServerCommandTeleporter;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.strategy.OWorkflowStrategy;
import com.orientechnologies.teleporter.ui.OProgressMonitor;
import com.orientechnologies.teleporter.util.ODriverConfigurator;
import com.orientechnologies.teleporter.util.OMigrationConfigManager;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Main Class from which the importing process starts.
 *
 * @author Gabriele Ponzi <<<<<<< Updated upstream
 * @email <g.ponzi--at--orientdb.com>
 * <p>
 * =======
 * @email <gabriele.ponzi--at--gmail.com> >>>>>>> Stashed changes
 */

public class OTeleporter extends OServerPluginAbstract {

  private static OOutputStreamManager outputManager;

  private static final OStrategyFactory FACTORY = new OStrategyFactory();

  private static final String teleport =
      "OrientDB                  \n" + " ______________________________________________________________________________ \n"
          + " ___  __/__  ____/__  /___  ____/__  __ \\_  __ \\__  __\\__  __/__  ____/__  _ _ \\  \n"
          + " __  /  __  __/  __  / __  __/  __  /_/ /  / / /_  /_/ /_  /  __  __/  __  /_/ /\n"
          + " _  /   _  /___  _  /___  /___  _  ____// /_/ /_  _, _/_  /   _  /___  _  _, _/ \n"
          + " /_/    /_____/  /_____/_____/  /_/     \\____/ /_/ |_| /_/    /_____/  /_/ |_|  \n" + "\n"
          + "                                                  http://orientdb.com/teleporter";
  private OServer server;

  public static void main(String[] args) throws Exception {

    // Output Manager setting
    outputManager = new OOutputStreamManager(2);
    outputManager.info("\n\n" + teleport + "\n\n");

    /*
     * Input args validation
     */

    // Missing argument validation

    if (args.length < 6) {
      outputManager.error(
          "Syntax error, missing argument. Use:\n ./oteleporter.sh -jdriver <jdbc-driver> -jurl <jdbc-url> -juser <username> -jpasswd <password> -ourl <orientdb-url>.\n");
      throw new OTeleporterIOException();
    }

    // Filling the argument map
    Map<String, String> arguments = new HashMap<String, String>();

    int i = 0;
    while (i < args.length) {
      arguments.put(args[i], args[i + 1]);
      i += 2;
    }

    // Mandatory args validation

    /*
    if (!arguments.containsKey("-jdriver")) {
      outputManager
              .error("Argument -jdriver is mandatory, please try again with expected argument: -jdriver <your-db-driver-name>\n");
      throw new OTeleporterIOException();
    }

    if (!arguments.containsKey("-jurl")) {
      outputManager.error("Argument -jurl is mandatory, please try again with expected argument: -jurl <input-db-jdbc-URL>\n");
      throw new OTeleporterIOException();
    }*/

    if (!arguments.containsKey("-ourl")) {
      outputManager
          .error("Argument -ourl is mandatory, please try again with expected argument: -ourl <output-orientdb-desired-URL>\n");
      throw new OTeleporterIOException();
    }

    // simple syntax check on command
    if (arguments.get("-jdriver") != null) {
      if (!arguments.get("-jdriver").equalsIgnoreCase("Oracle") && !arguments.get("-jdriver").equalsIgnoreCase("SQLServer")
          && !arguments.get("-jdriver").equalsIgnoreCase("MySQL") && !arguments.get("-jdriver").equalsIgnoreCase("PostgreSQL")
          && !arguments.get("-jdriver").equalsIgnoreCase("HyperSQL")) {
        outputManager.error(
            "Not valid db-driver name. Type one of the following driver names: 'Oracle','SQLServer','MySQL','PostgreSQL','HyperSQL'\n");
        throw new OTeleporterIOException();
      }
    }

    if (arguments.get("-jurl") != null) {
      if (!arguments.get("-jurl").contains("jdbc:")) {
        outputManager.error("Not valid db-url.\n");
        throw new OTeleporterIOException();
      }
    }

    if (!(arguments.get("-ourl").contains("plocal:") | arguments.get("-ourl").contains("remote:") | arguments.get("-ourl")
        .contains("memory:"))) {
      outputManager.error("Not valid output orient db uri.\n");
      throw new OTeleporterIOException();
    }

    if (arguments.get("-s") != null) {
      if (!(arguments.get("-s").equals("naive") | arguments.get("-s").equals("naive-aggregate"))) {
        outputManager.error("Not valid strategy.\n");
        throw new OTeleporterIOException();
      }
    }

    if (arguments.get("-v") != null) {
      if (!(arguments.get("-v").equals("0") | arguments.get("-v").equals("1") | arguments.get("-v").equals("2") | arguments
          .get("-v").equals("3"))) {
        outputManager
            .error("Not valid output level. Available levels:\n0 - No messages\n1 - Debug\n2 - Info\n3 - Warning \n4 - Error\n");
        throw new OTeleporterIOException();
      }
    }

    if (arguments.get("-inheritance") != null) {
      if (!(arguments.get("-inheritance").contains("hibernate:"))) {
        outputManager.error("Not valid inheritance argument. Syntax: -inheritance hibernate:<xml-path>\n");
        throw new OTeleporterIOException();
      }
    }

    if (arguments.get("-include") != null && arguments.get("-exclude") != null) {
      outputManager.error("It's not possible to use both 'include' and 'exclude' arguments.\n");
      throw new OTeleporterIOException();
    }

    if (arguments.get("-conf") != null) {
      File file = new File(arguments.get("-conf"));
      try {
        file.getCanonicalPath();
      } catch (IOException e) {
        outputManager.error("Configuration file path not valid.\n");
        throw new OTeleporterIOException(e);
      }
    }

    // Mandatory arguments
    String driver = arguments.get("-jdriver");
    String jurl = arguments.get("-jurl");
    String outDbUrl = arguments.get("-ourl");

    // Optional arguments
    String username = arguments.get("-juser");
    String password = arguments.get("-jpasswd");
    String chosenStrategy = arguments.get("-s");
    String nameResolver = arguments.get("-nr");
    String outputLevel = arguments.get("-v");
    String chosenMapper = "basicDBMapper"; // Mapper argument
    String xmlPath = null;
    if (arguments.containsKey("-inheritance")) {
      String argument = arguments.get("-inheritance");

      chosenMapper = argument.substring(0, argument.indexOf(':'));
      xmlPath = argument.substring(argument.indexOf(':') + 1);
    }

    List<String> includedTables = null;
    List<String> excludedTables = null;

    if (arguments.get("-include") != null) {
      String tables = arguments.get("-include");
      String[] arrayTables = tables.split(",");
      includedTables = new ArrayList<String>(Arrays.asList(arrayTables));
    }

    if (arguments.get("-exclude") != null) {
      String tables = arguments.get("-exclude");
      String[] arrayTables = tables.split(",");
      excludedTables = new ArrayList<String>(Arrays.asList(arrayTables));
    }
    String configurationPath = arguments.get("-conf");

    OTeleporter
        .execute(driver, jurl, username, password, outDbUrl, chosenStrategy, chosenMapper, xmlPath, nameResolver, outputLevel,
            includedTables, excludedTables, configurationPath, outputManager);
  }

  /**
   * Executes the import of the source DB in a OrientDB Graph through different parameters.
   *
   * @param driver            the driver name of the DBMS from which you want to execute the import
   * @param jurl              an absolute URL giving the location of the source DB to import
   * @param username          to access to the source DB
   * @param password          to access to the source DB
   * @param chosenStrategy    the execution approach adopted during the importing of data
   * @param outDbUrl          an absolute URI for the destination Orient Graph DB
   * @param nameResolver      the name of the resolver which transforms the names of all the elements of the source DB according to
   *                          a specific convention (if null Java convention is adopted)
   * @param outputLevel       the level of the logging messages that will be printed on the OutputStream during the execution
   * @param excludedTables
   * @param includedTables
   * @param configurationPath
   *
   * @throws OTeleporterIOException
   */

  public static void execute(String driver, String jurl, String username, String password, String outDbUrl, String chosenStrategy,
      String chosenMapper, String xmlPath, String nameResolver, String outputLevel, List<String> includedTables,
      List<String> excludedTables, String configurationPath, OOutputStreamManager outputManager) throws OTeleporterIOException {

    // trying to load the configuration starting from the input configurationPath
    ODocument migrationConfigDoc = null;
    String jsonMigrationConfig = null;
    if (configurationPath != null) {
      migrationConfigDoc = OMigrationConfigManager.loadMigrationConfigFromFile(configurationPath);
      if (migrationConfigDoc != null) {
        jsonMigrationConfig = migrationConfigDoc.toJSON("");
      } else {
        OTeleporterContext.getInstance().getOutputManager().info(
            "No migration configuration file was found in the suggested path. Migration will be performed according "
                + "to standard mapping rules or to the latest configured policies if any.\n");
      }
    }

    executeJob(driver, jurl, username, password, outDbUrl, chosenStrategy, chosenMapper, xmlPath, nameResolver, outputLevel,
        includedTables, excludedTables, jsonMigrationConfig, outputManager);
  }

  /**
   * Executes the import of the source DB in a OrientDB Graph through different parameters.
   *
   * @param driver              the driver name of the DBMS from which you want to execute the import
   * @param jurl                an absolute URL giving the location of the source DB to import
   * @param username            to access to the source DB
   * @param password            to access to the source DB
   * @param chosenStrategy      the execution approach adopted during the importing of data
   * @param outDbUrl            an absolute URI for the destination Orient Graph DB
   * @param nameResolver        the name of the resolver which transforms the names of all the elements of the source DB according
   *                            to a specific convention (if null Java convention is adopted)
   * @param outputLevel         the level of the logging messages that will be printed on the OutputStream during the execution
   * @param excludedTables
   * @param includedTables
   * @param jsonMigrationConfig
   *
   * @throws OTeleporterIOException
   */

  public static ODocument executeJob(String driver, String jurl, String username, String password, String outDbUrl,
      String chosenStrategy, String chosenMapper, String xmlPath, String nameResolver, String outputLevel,
      List<String> includedTables, List<String> excludedTables, String jsonMigrationConfig, OOutputStreamManager outputManager)
      throws OTeleporterIOException {

    // REGISTER THE BINARY RECORD SERIALIZER TO SUPPORT ANY OF THE EXTERNAL FIELDS
    ORecordSerializerFactory.instance().register("ORecordSerializerBinary", new ORecordSerializerBinary());

    OTeleporterContext.newInstance().setOutputManager(outputManager);
    ODriverConfigurator driverConfig = new ODriverConfigurator();
    List<OSourceDatabaseInfo> sourcesInfo = null;
    boolean sourceInfoLoaded = false;

    if (driver == null || jurl == null) {

      // try to get args from config files in the target orientdb db (if already present)
      ODocument sourcesInfoDoc = OMigrationConfigManager.loadSourceInfo(outDbUrl);
      if (sourcesInfoDoc == null) {
        outputManager.error(
            "Arguments -jdriver, -jurl, -juser and -jpasswd, necessary to access the source databases, were not specified and "
                + "no previous sources's info were found in the target OrientDB graph database.\n");
        throw new OTeleporterIOException();
      } else {
        sourceInfoLoaded = true;
        sourcesInfo = OMigrationConfigManager.extractSourceDatabaseInfo(sourcesInfoDoc);
      }
    } else {
      String driverClassName = driverConfig.fetchDriverClassName(driver);
      OSourceDatabaseInfo sourceDBInfo = new OSourceDatabaseInfo(driver, driverClassName, jurl, username, password);
      sourcesInfo = new LinkedList<OSourceDatabaseInfo>();
      sourcesInfo.add(sourceDBInfo);
    }
    // checking driver configuration
    driverConfig.checkDriverConfiguration(sourcesInfo.get(0).getSourceIdName());

    /**
     * Handling configuration files (source access info and migration configuration file)
     */

    // fetching the first source access info (now Teleporter is conceived to accept just a source info for the migration)
    OSourceDatabaseInfo sourceInfo = sourcesInfo.get(0);

    // migration configuration
    ODocument migrationConfig = null;
    if (jsonMigrationConfig != null && jsonMigrationConfig.length() > 0) {

      // use this migration and write it in the target database
      migrationConfig = new ODocument();
      migrationConfig.fromJSON(jsonMigrationConfig, "noMap");
      //OMigrationConfigManager.writeConfigurationInTargetDB(migrationConfig, outDbUrl);
    } else {
      // try to load a previous file configuration in the target db
      OTeleporterContext.getInstance().getOutputManager()
          .info("\nTrying to load a previous configuration file in the target OrientDB database...\n");
      String configurationPath = OMigrationConfigManager
          .buildConfigurationFilePath(outDbUrl, OMigrationConfigManager.getConfigFileName());
      String configDirPath = configurationPath.substring(0, configurationPath.lastIndexOf("/") + 1);
      migrationConfig = OMigrationConfigManager.loadMigrationConfigFromFile(configurationPath);
      // if present use it
      if (migrationConfig != null) {
        OTeleporterContext.getInstance().getOutputManager()
            .info("A previous configuration in the %s path was loaded and it will be used for the current migration.",
                configDirPath);
      }
      // else no migration will be used for the migration/sync
      else {
        OTeleporterContext.getInstance().getOutputManager().info(
            "No previous configuration in the %s path was found.\nMigration will be performed according to standard mapping rules.\n\n",
            configurationPath);
      }
    }

    // Disabling query scan threshold tip
    OGlobalConfiguration.QUERY_SCAN_THRESHOLD_TIP.setValue(-1);

    // OutputStream setting

    if (outputLevel != null)
      outputManager.setLevel(Integer.parseInt(outputLevel));

    // Progress Monitor initialization
    OProgressMonitor progressMonitor = new OProgressMonitor();
    progressMonitor.initialize();

    // DB Query engine building
    ODBQueryEngine dbQueryEngine = new ODBQueryEngine(sourceInfo.getDriverName());
    OTeleporterContext.getInstance().setDbQueryEngine(dbQueryEngine);

    OWorkflowStrategy strategy = FACTORY.buildStrategy(driver, chosenStrategy);
    ODocument executionResult;

    // Timer for statistics notifying
    Timer timer = new Timer();
    try {
      timer.scheduleAtFixedRate(new TimerTask() {

        @Override
        public void run() {
          OTeleporterContext.getInstance().getStatistics().notifyListeners();
        }
      }, 0, 1000);

      // the last argument represents the nameResolver
      executionResult = strategy
          .executeStrategy(sourceInfo, outDbUrl, chosenMapper, xmlPath, nameResolver, includedTables, excludedTables,
              migrationConfig);

      // Disabling query scan threshold tip
      OGlobalConfiguration.QUERY_SCAN_THRESHOLD_TIP.setValue(50000);

      // Writing sources access info
      if (!sourceInfoLoaded) {
        OMigrationConfigManager.upsertSourceDatabaseInfo(sourcesInfo, outDbUrl);
      }
      // Writing last configuration
      if (executionResult != null) {
        OMigrationConfigManager.writeConfigurationInTargetDB(executionResult, outDbUrl);
      }

    } finally {
      timer.cancel();

    }
    return executionResult;
  }

  public static ODocument execute(String driver, String jurl, String username, String password, String outDbUrl,
      String chosenStrategy, String chosenMapper, String xmlPath, String nameResolver, String outputLevel,
      List<String> includedTables, List<String> excludedTables, OOutputStreamManager outputManager) throws OTeleporterIOException {

    return executeJob(driver, jurl, username, password, outDbUrl, chosenStrategy, chosenMapper, xmlPath, nameResolver, outputLevel,
        includedTables, excludedTables, null, outputManager);
  }

  @Override
  public String getName() {
    return "teleporter";
  }

  @Override
  public void startup() {

    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.registerStatelessCommand(new OServerCommandTeleporter());
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    server = oServer;
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}