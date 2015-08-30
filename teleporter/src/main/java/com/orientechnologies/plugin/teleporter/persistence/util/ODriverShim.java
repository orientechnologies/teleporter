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