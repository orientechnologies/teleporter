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

package com.orientechnologies.orient.drakkar.importengine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.model.dbschema.OEntity;
import com.orientechnologies.orient.drakkar.persistence.util.ODBSourceConnection;

/**
 * Implementation of ODataSourceQueryEngine. It executes the necessary queries for the source DB records fetching.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODBQueryEngine implements ODataSourceQueryEngine {

  private ODBSourceConnection dataSource;
  private Connection dbConnection;
  private PreparedStatement statement;
  private ResultSet results;

  public ODBQueryEngine(String driver, String uri, String username, String password) {
    this.dataSource =  new ODBSourceConnection(driver, uri, username, password);
  }

  public ResultSet getRecordsByEntity(String entityName, ODrakkarContext context) {

    this.results = null;
    this.dbConnection = null;
    this.statement = null;
    String query = "select * from " + entityName;

    try {

      try {
        this.dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      this.statement = dbConnection.prepareStatement(query);
      results = statement.executeQuery();

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    return results;
  }

  /**
   * @param currentDiscriminatorValue
   */
  public ResultSet getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, String entityName, ODrakkarContext context) {

    this.results = null;
    this.dbConnection = null;
    this.statement = null;
    String query = "select * from " + entityName + " where " + discriminatorColumn + "='" + currentDiscriminatorValue + "'";

    try {

      try {
        this.dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      this.statement = dbConnection.prepareStatement(query);
      results = statement.executeQuery();

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    return results;
  }


  public ResultSet getEntityTypeFromSingleTable(String discriminatorColumn, String physicalEntityName, String[] propertyOfKey, String[] valueOfKey, ODrakkarContext context) {
    this.results = null;
    this.dbConnection = null;
    this.statement = null;
    String query = "select " + discriminatorColumn + " from " + physicalEntityName + " where ";
    
    query += propertyOfKey[0] + " = " + valueOfKey[0];
    
    if(propertyOfKey.length > 1) {
      for(int i=1; i<propertyOfKey.length; i++) {
        query += " and " + propertyOfKey[i] + " = " + valueOfKey[i];
      }
    }
    
    try {

      try {
        this.dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      this.statement = dbConnection.prepareStatement(query);
      results = statement.executeQuery();

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    return results;

  }


  public void closeAll(ODrakkarContext context) {

    try {
      if(this.dbConnection != null) 
        this.dbConnection.close();
      if(this.statement != null) 
        this.statement.close();
      if(this.results != null)
        this.results.close();
    } catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }

  }

}
