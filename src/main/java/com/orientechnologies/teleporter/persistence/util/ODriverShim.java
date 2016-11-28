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

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

class ODriverShim implements Driver {

  private Driver driver;

  ODriverShim(Driver d) {
    this.driver = d;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return this.driver.acceptsURL(url);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return this.driver.connect(url, info);
  }

  @Override
  public int getMajorVersion() {
    return this.driver.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return this.driver.getMinorVersion();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return this.driver.getPropertyInfo(url, info);
  }

  @Override
  public boolean jdbcCompliant() {
    return this.driver.jdbcCompliant();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO Auto-generated method stub
    return null;
  }




}