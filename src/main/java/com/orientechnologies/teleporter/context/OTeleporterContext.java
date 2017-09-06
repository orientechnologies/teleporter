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

package com.orientechnologies.teleporter.context;

import com.orientechnologies.orient.output.OPluginMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODriverDataTypeHandler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Context class for Drakkar execution.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OTeleporterContext {

  private static OTeleporterContext instance = null;

  private OTeleporterStatistics  statistics;
  private OPluginMessageHandler messageHandler;
  private ODriverDataTypeHandler dataTypeHandler;
  private ONameResolver          nameResolver;
  private String                 driverDependencyPath;
  private String                 executionStrategy;
  private ODBQueryEngine         dbQueryEngine;

  private OTeleporterContext() {
    this.statistics = new OTeleporterStatistics();
  }

  public static OTeleporterContext getInstance() {
    if (instance == null) {
      instance = new OTeleporterContext();
    }
    return instance;
  }

  public static OTeleporterContext newInstance() {
    instance = new OTeleporterContext();
    return instance;
  }

  public OTeleporterStatistics getStatistics() {
    return this.statistics;
  }

  public void setStatistics(OTeleporterStatistics statistics) {
    this.statistics = statistics;
  }

  public OPluginMessageHandler getMessageHandler() {
    return this.messageHandler;
  }

  public void setMessageHandler(OPluginMessageHandler messageHandler) {
    this.messageHandler = messageHandler;
  }

  public ODriverDataTypeHandler getDataTypeHandler() {
    return this.dataTypeHandler;
  }

  public void setDataTypeHandler(ODriverDataTypeHandler dataTypeHandler) {
    this.dataTypeHandler = dataTypeHandler;
  }

  public ONameResolver getNameResolver() {
    return this.nameResolver;
  }

  public void setNameResolver(ONameResolver nameResolver) {
    this.nameResolver = nameResolver;
  }

  public String getDriverDependencyPath() {
    return this.driverDependencyPath;
  }

  public void setDriverDependencyPath(String driverDependencyPath) {
    this.driverDependencyPath = driverDependencyPath;
  }

  public String getExecutionStrategy() {
    return this.executionStrategy;
  }

  public void setExecutionStrategy(String executionStrategy) {
    this.executionStrategy = executionStrategy;
  }

  public ODBQueryEngine getDbQueryEngine() {
    return dbQueryEngine;
  }

  public void setDbQueryEngine(ODBQueryEngine dbQueryEngine) {
    this.dbQueryEngine = dbQueryEngine;
  }

  /**
   * Prints the error message for a caught exception according to a level passed as argument. It's composed of:
   * - defined error message
   * - exception message
   *
   * @param e
   * @param message
   * @param level
   *
   * @return printedMessage
   */
  public String printExceptionMessage(Exception e, String message, String level) {

    if (e.getMessage() != null)
      message += "\n" + e.getClass().getName() + " - " + e.getMessage();
    else
      message += "\n" + e.getClass().getName();

    switch (level) {
    case "debug":
      this.messageHandler.debug(message);
      break;
    case "info":
      this.messageHandler.info(message);
      break;
    case "warn":
      this.messageHandler.warn(message);
      break;
    case "error":
      this.messageHandler.error(message);
      break;
    }

    return message;
  }

  /**
   * Builds the exception stack trace and prints it according to a level passed as argument.
   *
   * @param e
   * @param level
   *
   * @return printedMessage
   */
  public String printExceptionStackTrace(Exception e, String level) {

    // copying the exception stack trace in the string
    Writer writer = new StringWriter();
    e.printStackTrace(new PrintWriter(writer));
    String s = writer.toString();

    switch (level) {
    case "debug":
      this.messageHandler.debug("\n" + s + "\n");
      break;
    case "info":
      this.messageHandler.info("\n" + s + "\n");
      break;
    case "warn":
      this.messageHandler.warn("\n" + s + "\n");
      break;
    case "error":
      this.messageHandler.error("\n" + s + "\n");
      break;
    }

    return s;
  }

}
