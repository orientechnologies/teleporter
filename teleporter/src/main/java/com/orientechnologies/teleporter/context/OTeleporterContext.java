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

}
