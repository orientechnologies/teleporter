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

package com.orientechnologies.orient.drakkar.persistence.util;

import java.sql.Connection;
import java.sql.SQLException;

import javax.persistence.PersistenceException;

import oracle.jdbc.pool.OracleDataSource;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;

/**
 * Utility class to which connection with source DB is delegated.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class OOracleDataSource {

  private String uri;
  private String username;
  private String password;


  public OOracleDataSource(String driver, String uri, String username, String password) {	
    this.uri = uri;
    this.username = username;
    this.password = password;

  }

  public Connection getConnection(ODrakkarContext context)  {
    Connection connection = null;
    context.getOutputManager().debug("Attempting connection to " + this.uri + " ...");
    try {
      OracleDataSource ods = new OracleDataSource();
      ods.setURL(this.uri);
//      ods.setDriverType("thin");
//      ods.setNetworkProtocol("tcp");
//      ods.setPortNumber(1521);
      ods.setDatabaseName("avnown");
//      ods.setUser("avnown");
//      ods.setPassword("avnown");
      connection = ods.getConnection(username, password);
      context.getOutputManager().debug("Successful connection.\n");

    } catch(SQLException e) {
      context.getOutputManager().error("SQLException during connection attempting.\n");
      throw new PersistenceException(e.getMessage());

    }
    return connection;

  }
}
