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

import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODriverDataTypeHandler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Context class for Drakkar execution.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OTeleporterContext {

  private OTeleporterStatistics  statistics;
  private OOutputStreamManager   outputManager;
  private ODriverDataTypeHandler dataTypeHandler;
  private String                 queryQuote;
  private ONameResolver          nameResolver;
  private String                 driverDependencyPath;
  private String                 executionStrategy;

  // TO DELETE !!!!
  private String aggregationFunction;
  private String[][] columns;

  public String getAggregationFunction() {
    return aggregationFunction;
  }

  public void setAggregationFunction(String aggregationFunction) {
    this.aggregationFunction = aggregationFunction;
  }

  public String[][] getColumns() {
    return columns;
  }

  public void setColumns(String[][] columns) {
    this.columns = columns;
  }
  // TO DELETE !!!!

  public OTeleporterContext() {
    this.statistics = new OTeleporterStatistics();
  }

  public OTeleporterStatistics getStatistics() {
    return this.statistics;
  }

  public void setStatistics(OTeleporterStatistics statistics) {
    this.statistics = statistics;
  }

  public OOutputStreamManager getOutputManager() {
    return this.outputManager;
  }

  public void setOutputManager(OOutputStreamManager outputManager) {
    this.outputManager = outputManager;
  }

  public ODriverDataTypeHandler getDataTypeHandler() {
    return this.dataTypeHandler;
  }

  public void setDataTypeHandler(ODriverDataTypeHandler dataTypeHandler) {
    this.dataTypeHandler = dataTypeHandler;
  }

  public String getQueryQuote() {
    return this.queryQuote;
  }

  public void setQueryQuoteType(String queryQuoteType) {
    this.queryQuote = queryQuoteType;
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

  /**
   * Prints the error message for a caught exception according to a level passed as argument. It's composed of:
   * - defined error message
   * - exception message
   * @param e
   * @param message
   * @param level
   * @return printedMessage
   */
  public String printExceptionMessage(Exception e, String message, String level) {

    if(e.getMessage() != null)
      message += "\n" + e.getClass().getName() + " - " + e.getMessage();
    else
      message += "\n" + e.getClass().getName();

    switch(level) {
    case "debug": this.outputManager.debug(message);
      break;
    case "info": this.outputManager.info(message);
      break;
    case "warn": this.outputManager.warn(message);
      break;
    case "error": this.outputManager.error(message);
      break;
    }

    return message;
  }

  /**
   * Builds the exception stack trace and prints it according to a level passed as argument.
   * @param e
   * @param level
   * @return printedMessage
   */
  public String printExceptionStackTrace(Exception e, String level) {

    // copying the exception stack trace in the string
    Writer writer = new StringWriter();
    e.printStackTrace(new PrintWriter(writer));
    String s = writer.toString();

    switch(level) {
    case "debug": this.outputManager.debug("\n" + s + "\n");
      break;
    case "info": this.outputManager.info("\n" + s + "\n");
      break;
    case "warn": this.outputManager.warn("\n" + s + "\n");
      break;
    case "error": this.outputManager.error("\n" + s + "\n");
      break;
    }

    return s;
  }

}
