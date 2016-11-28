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

package com.orientechnologies.teleporter.importengine.rdbms;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.ODataSourceQueryEngine;
import com.orientechnologies.teleporter.model.dbschema.OAttribute;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;
import com.orientechnologies.teleporter.persistence.util.ODBSourceConnection;
import com.orientechnologies.teleporter.persistence.util.OQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

/**
 * Implementation of ODataSourceQueryEngine. It executes the necessary queries for the source DB records fetching.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODBQueryEngine implements ODataSourceQueryEngine {

  private ODBSourceConnection dataSource;
  private String              quote;


  public ODBQueryEngine(String driver, String uri, String username, String password, OTeleporterContext context) {
    this.dataSource =  new ODBSourceConnection(driver, uri, username, password);
    this.quote = context.getQueryQuote();
  }


  /**
   * @param entityName
   * @param propertyOfKey
   * @param valueOfKey
   * @param context
   * @return
   */
  public OQueryResult getRecordById(String entityName, String entitySchema, String[] propertyOfKey, String[] valueOfKey, OTeleporterContext context) {

    ResultSet aggregateTable = null;
    Connection dbConnection = null;
    Statement statement = null;
    String query;

    if(entitySchema != null) 
      query = "select * from " + entitySchema + "." + this.quote + entityName + this.quote + " where ";
    else
      query = "select * from " + this.quote + entityName + this.quote + " where ";

    query += this.quote + propertyOfKey[0] + this.quote + " = '" + valueOfKey[0] + "'";


    if(propertyOfKey.length > 1) {
      for(int i=1; i<propertyOfKey.length; i++) {
        query += " and " + this.quote + propertyOfKey[i] + this.quote + " = '" + valueOfKey[i] + "'";
      }
    }

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        String mess = "";
        context.printExceptionMessage(e, mess, "error");
        context.printExceptionStackTrace(e, "debug");
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      aggregateTable = statement.executeQuery(query);

    } catch (SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, aggregateTable);
    return queryResult;
  }



  public OQueryResult getRecordsByEntity(String entityName, String entitySchema, OTeleporterContext context) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;

    String query;
    if(entitySchema != null)
      query = "select * from " + entitySchema + "." + this.quote + entityName + this.quote;
    else
      query = "select * from " + this.quote + entityName + this.quote;

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        String mess = "";
        context.printExceptionMessage(e, mess, "error");
        context.printExceptionStackTrace(e, "debug");
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    } catch (SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }

  public OQueryResult getRecordsByQuery(String query, OTeleporterContext context) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;

    try {
      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        String mess = "";
        context.printExceptionMessage(e, mess, "error");
        context.printExceptionStackTrace(e, "debug");
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    } catch (SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }

  /**
   * @param currentDiscriminatorValue
   */
  public OQueryResult getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, String entityName, String entitySchema, OTeleporterContext context) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;

    String query;
    if(entitySchema != null)
      query = "select * from " + entitySchema + "." + this.quote + entityName + this.quote;
    else
      query = "select * from " + this.quote + entityName + this.quote;

    query += " where " + this.quote + discriminatorColumn + this.quote + "='" + currentDiscriminatorValue + "'";

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        String mess = "";
        context.printExceptionMessage(e, mess, "error");
        context.printExceptionStackTrace(e, "debug");
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    } catch (SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }


  public OQueryResult getEntityTypeFromSingleTable(String discriminatorColumn, String physicalEntityName, String entitySchema, String[] propertyOfKey, String[] valueOfKey, OTeleporterContext context) {
    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;

    String query;
    if(entitySchema != null)
      query = "select " + discriminatorColumn + " from " + entitySchema + "." + this.quote + physicalEntityName + this.quote + " where ";
    else
      query = "select " + discriminatorColumn + " from " + this.quote + physicalEntityName + this.quote + " where ";

    query += this.quote + propertyOfKey[0] + this.quote + " = '" + valueOfKey[0] + "'";

    if(propertyOfKey.length > 1) {
      for(int i=1; i<propertyOfKey.length; i++) {
        query += " and " + this.quote + propertyOfKey[i] + this.quote + " = '" + valueOfKey[i] + "'";
      }
    }

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        String mess = "";
        context.printExceptionMessage(e, mess, "error");
        context.printExceptionStackTrace(e, "debug");
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    } catch (SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;

  }


  /**
   * @param bag
   * @return
   */
  public OQueryResult buildAggregateTableFromHierarchicalBag(OHierarchicalBag bag, OTeleporterContext context) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;

    Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
    OEntity rootEntity = it.next();

    String query;
    if(rootEntity.getSchemaName() != null)
      query = "select * from " + rootEntity.getSchemaName() + "." + this.quote + rootEntity.getName() + this.quote + " as t0\n";
    else
      query = "select * from " + this.quote + rootEntity.getName() + this.quote + " as t0\n";

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

        if(currentEntity.getSchemaName() != null) 
          query += "left join " + currentEntity.getSchemaName() + "." + this.quote + currentEntity.getName() + this.quote;
        else
          query += "left join " + this.quote + currentEntity.getName() + this.quote; 

        query += " as t" + thTable + 
            " on t0." + this.quote + rootEntityPropertyOfKey[0] + this.quote + " = t" + thTable + "." + this.quote + currentEntityPropertyOfKey[0] + this.quote;

        for(int k=1; k<currentEntityPropertyOfKey.length; k++) {
          query += " and " + this.quote + rootEntityPropertyOfKey[k] + this.quote + " = t" + thTable + "." + this.quote + currentEntityPropertyOfKey[0] + this.quote;
        }

        query += "\n";
        thTable++;
      }
    }

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        String mess = "";
        context.printExceptionMessage(e, mess, "error");
        context.printExceptionStackTrace(e, "debug");
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    } catch (SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }

}
