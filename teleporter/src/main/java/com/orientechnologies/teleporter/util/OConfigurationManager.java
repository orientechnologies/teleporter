package com.orientechnologies.teleporter.util;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;

import java.io.*;
import java.nio.channels.FileChannel;

/**
 * Created by gabriele on 06/06/16.
 */
public class OConfigurationManager {


  // config info
  private final String configurationDirectoryName = "teleporter-config/";
  private final String configFileName = "migration-config.json";
  private String outDBConfigPath;       // path ORIENTDB_HOME/<db-name>/teleporter-config/config.json
  private boolean configPresentInDB;

  public OConfigurationManager() {}


  /**
   * Loading eventual configuration.
   * Look for the config in the <db-path>/teleporter-config/ path:
   *  (i) - if db and configuration are present use the default config
   *      - else
   *       (ii)  - if an external config path was passed as argument then load the config, use it for the steps 1,2,3 and then copy it
   *               in the <db-path>/teleporter-config/ path (configuration.json)
   *       (iii) - else execute strategy without configuration
   **/
  public ODocument loadConfiguration(String outOrientGraphUri, String configurationPath, OTeleporterContext context) {

    if(outOrientGraphUri.contains("\\")) {
      outOrientGraphUri = outOrientGraphUri.replace("\\","/");
    }

    // checking the presence of the configuration in the target db
    if (!(outOrientGraphUri.charAt(outOrientGraphUri.length() - 1) == '/')) {
      outOrientGraphUri += "/";
    }
    this.outDBConfigPath = outOrientGraphUri + this.configurationDirectoryName + this.configFileName;
    this.outDBConfigPath = this.outDBConfigPath.replace("plocal:", "");
    File confFileInOrientDB = new File(this.outDBConfigPath);

    if (confFileInOrientDB.exists())
      this.configPresentInDB = true;
    else
      this.configPresentInDB = false;

    // (i)
    ODocument config = null;
    try {
      if (this.configPresentInDB) {
        config = OFileManager.buildJsonFromFile(this.outDBConfigPath);
        context.getOutputManager().info("Configuration correctly loaded from %s.\n", this.outDBConfigPath);
      } else {
        config = OFileManager.buildJsonFromFile(configurationPath);
        // (ii)
        if (config != null) {
          context.getOutputManager().info("Configuration correctly loaded from %s and saved in %s.\n", configurationPath, this.outDBConfigPath);

          // manage conf if present: updating in the db directory
          this.copyConfigurationInDatabase(config, configurationPath, context);
        }
        // (iii)
        else {
          context.getOutputManager().info("No configuration file was found. Migration will be performed with standard mapping policies.\n");
        }
      }
    } catch(Exception e) {
      String mess = "";
      if(e.getMessage() != null)
        mess += "\n" + e.getClass().getName() + " - " + e.getMessage();
      else
        mess += "\n" + e.getClass().getName();

      context.getOutputManager().error(mess);

      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      String s = writer.toString();
      context.getOutputManager().debug("\n" + s + "\n");
      throw new OTeleporterRuntimeException(e);
    }

    return config;
  }

  public void copyConfigurationInDatabase(ODocument config, String configurationPath, OTeleporterContext context) {

    // if we have config in input and it is not present in the DB then we copy it in the <db-path>/teleporter-config/ path
    if (config != null) {
      if (!this.configPresentInDB) {
        try {
          this.copyConfigIntoTargetDB(configurationPath, this.outDBConfigPath);
        } catch (IOException e) {
          String mess = "";
          if(e.getMessage() != null)
            mess += "\n" + e.getClass().getName() + " - " + e.getMessage();
          else
            mess += "\n" + e.getClass().getName();

          context.getOutputManager().error(mess);

          Writer writer = new StringWriter();
          e.printStackTrace(new PrintWriter(writer));
          String s = writer.toString();
          context.getOutputManager().error("\n" + s + "\n");
          throw new OTeleporterRuntimeException(e);
        }
      }
    }
  }

  private void copyConfigIntoTargetDB(String sourceConfigPath, String destinationConfigPath) throws IOException {

    File sourceConfig = new File(sourceConfigPath);
    File destinationConfig = new File(destinationConfigPath);
    destinationConfig.getParentFile().mkdirs();
    destinationConfig.createNewFile();
    FileChannel in = new FileInputStream(sourceConfig).getChannel();
    FileChannel out = new FileOutputStream(destinationConfig).getChannel();
    out.transferFrom(in, 0, in.size());
  }
}
