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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.model.dbschema.OAttribute;
import com.orientechnologies.orient.drakkar.model.dbschema.OEntity;
import com.orientechnologies.orient.drakkar.model.dbschema.OHierarchicalBag;
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
  private Statement statement;
  public ResultSet results;
  public ResultSet aggregateTable;

  public ODBQueryEngine(String driver, String uri, String username, String password) {
    this.dataSource =  new ODBSourceConnection(driver, uri, username, password);
  }
  
  
  /**
   * @param entityName
   * @param propertyOfKey
   * @param valueOfKey
   * @param context
   * @return
   */
  public ResultSet getRecordById(String entityName, String[] propertyOfKey, String[] valueOfKey, ODrakkarContext context) {
    
    this.aggregateTable = null;
    this.dbConnection = null;
    this.statement = null;
    String query = "select * from " + entityName + " where ";

    query += propertyOfKey[0] + " = '" + valueOfKey[0] + "'";

    if(propertyOfKey.length > 1) {
      for(int i=1; i<propertyOfKey.length; i++) {
        query += " and " + propertyOfKey[i] + " = '" + valueOfKey[i] + "'";
      }
    }

    try {

      try {
        this.dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      this.statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      this.aggregateTable = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    return this.aggregateTable;
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
      this.statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      this.results = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    return this.results;
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
      this.statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      this.results = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    return this.results;
  }


  public ResultSet getEntityTypeFromSingleTable(String discriminatorColumn, String physicalEntityName, String[] propertyOfKey, String[] valueOfKey, ODrakkarContext context) {
    this.results = null;
    this.dbConnection = null;
    this.statement = null;
    String query = "select " + discriminatorColumn + " from " + physicalEntityName + " where ";

    query += propertyOfKey[0] + " = '" + valueOfKey[0] + "'";

    if(propertyOfKey.length > 1) {
      for(int i=1; i<propertyOfKey.length; i++) {
        query += " and " + propertyOfKey[i] + " = '" + valueOfKey[i] + "'";
      }
    }

    try {

      try {
        this.dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      this.statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      this.results = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    return this.results;

  }
  

  /**
   * @param bag
   * @return
   */
  public ResultSet buildAggregateTableFromHierarchicalBag(OHierarchicalBag bag, ODrakkarContext context) {

    this.results = null;
    this.dbConnection = null;
    this.statement = null;

    Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
    OEntity rootEntity = it.next();

    String query = "select * from " + rootEntity.getName() + " as t0\n";

    String[] rootEntityPropertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];  // collects the attributes of the root-entity's primary key

    // filling the rootPropertyOfKey from the primary key of the rootEntity
    for(int j=0; j<rootEntity.getPrimaryKey().getInvolvedAttributes().size(); j++) {
      rootEntityPropertyOfKey[j] = rootEntity.getPrimaryKey().getInvolvedAttributes().get(j).getName();
    }

    String[] currentEntityPropertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];  // collects the attributes of the current-entity's primary key

    OEntity currentEntity;
    int thTable = 1;
    for(int i=1; i<bag.getDepth2entities().size(); i++) {
      it = bag.getDepth2entities().get(i).iterator();

      while(it.hasNext()) {

        currentEntity = it.next();
        int index = 0;
        for(OAttribute attribute: currentEntity.getPrimaryKey().getInvolvedAttributes()) {
          currentEntityPropertyOfKey[index] = attribute.getName();
          index++;
        }

        query += "left join " + currentEntity.getName() + " as t" + thTable + 
            " on t0." + rootEntityPropertyOfKey[0] + " = t" + thTable + "." + currentEntityPropertyOfKey[0];
        
        for(int k=1; k<currentEntityPropertyOfKey.length; k++) {
          query += " and " + rootEntityPropertyOfKey[k] + " = t" + thTable + "." + currentEntityPropertyOfKey[0];
        }
        
        query += "\n";
        thTable++;
      }
    }

    try {

      try {
        this.dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      this.statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      this.results = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    return this.results;
  }


  public void closeAll(ODrakkarContext context) {

    try {
      if(this.dbConnection != null && !this.dbConnection.isClosed()) 
        this.dbConnection.close();
      if(this.statement != null && !this.statement.isClosed()) 
        this.statement.close();
      if(this.results != null && !this.results.isClosed())
        this.results.close();
      if(this.aggregateTable != null && !this.aggregateTable.isClosed())
        this.aggregateTable.close();
    } catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }

  }

}
