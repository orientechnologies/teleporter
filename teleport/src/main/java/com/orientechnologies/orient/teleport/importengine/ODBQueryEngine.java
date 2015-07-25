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

package com.orientechnologies.orient.teleport.importengine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import com.orientechnologies.orient.teleport.context.OTeleportContext;
import com.orientechnologies.orient.teleport.model.dbschema.OAttribute;
import com.orientechnologies.orient.teleport.model.dbschema.OEntity;
import com.orientechnologies.orient.teleport.model.dbschema.OHierarchicalBag;
import com.orientechnologies.orient.teleport.persistence.util.ODBSourceConnection;
import com.orientechnologies.orient.teleport.persistence.util.OQueryResult;

/**
 * Implementation of ODataSourceQueryEngine. It executes the necessary queries for the source DB records fetching.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODBQueryEngine implements ODataSourceQueryEngine {

  private ODBSourceConnection dataSource;
  //  private Connection dbConnection;
  //  private Statement statement;
  //  public ResultSet results;
  //  public ResultSet aggregateTable;

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
  public OQueryResult getRecordById(String entityName, String[] propertyOfKey, String[] valueOfKey, OTeleportContext context) {

    ResultSet aggregateTable = null;
    Connection dbConnection = null;
    Statement statement = null;
    String query = "select * from " + entityName + " where ";

    query += propertyOfKey[0] + " = '" + valueOfKey[0] + "'";

    if(propertyOfKey.length > 1) {
      for(int i=1; i<propertyOfKey.length; i++) {
        query += " and " + propertyOfKey[i] + " = '" + valueOfKey[i] + "'";
      }
    }

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      aggregateTable = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, aggregateTable);
    return queryResult;
  }



  public OQueryResult getRecordsByEntity(String entityName, OTeleportContext context) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;
    String query = "select * from " + entityName;

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }

  /**
   * @param currentDiscriminatorValue
   */
  public OQueryResult getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, String entityName, OTeleportContext context) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;
    String query = "select * from " + entityName + " where " + discriminatorColumn + "='" + currentDiscriminatorValue + "'";

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }


  public OQueryResult getEntityTypeFromSingleTable(String discriminatorColumn, String physicalEntityName, String[] propertyOfKey, String[] valueOfKey, OTeleportContext context) {
    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;
    String query = "select " + discriminatorColumn + " from " + physicalEntityName + " where ";

    query += propertyOfKey[0] + " = '" + valueOfKey[0] + "'";

    if(propertyOfKey.length > 1) {
      for(int i=1; i<propertyOfKey.length; i++) {
        query += " and " + propertyOfKey[i] + " = '" + valueOfKey[i] + "'";
      }
    }

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;

  }


  /**
   * @param bag
   * @return
   */
  public OQueryResult buildAggregateTableFromHierarchicalBag(OHierarchicalBag bag, OTeleportContext context) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;

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
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        e.printStackTrace();
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().debug(e.getMessage());
      e.printStackTrace();
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }

}
