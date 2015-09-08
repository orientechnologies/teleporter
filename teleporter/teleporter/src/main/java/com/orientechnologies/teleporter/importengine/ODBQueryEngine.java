/*
 * Copyright 2015 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.teleporter.importengine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.model.dbschema.OAttribute;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;
import com.orientechnologies.teleporter.persistence.util.ODBSourceConnection;
import com.orientechnologies.teleporter.persistence.util.OQueryResult;

/**
 * Implementation of ODataSourceQueryEngine. It executes the necessary queries for the source DB records fetching.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODBQueryEngine implements ODataSourceQueryEngine {

  private ODBSourceConnection dataSource;

  
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
  public OQueryResult getRecordById(String entityName, String[] propertyOfKey, String[] valueOfKey, OTeleporterContext context) {

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
        context.getOutputManager().error(e.getMessage());
        Writer writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        context.getOutputManager().debug(writer.toString());
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      aggregateTable = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, aggregateTable);
    return queryResult;
  }



  public OQueryResult getRecordsByEntity(String entityName, OTeleporterContext context) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;
    String query = "select * from " + entityName;

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        context.getOutputManager().error(e.getMessage());
        Writer writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        context.getOutputManager().debug(writer.toString());
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }

  /**
   * @param currentDiscriminatorValue
   */
  public OQueryResult getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, String entityName, OTeleporterContext context) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;
    String query = "select * from " + entityName + " where " + discriminatorColumn + "='" + currentDiscriminatorValue + "'";

    try {

      try {
        dbConnection = dataSource.getConnection(context);
      } catch (Exception e) {
        context.getOutputManager().error(e.getMessage());
        Writer writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        context.getOutputManager().debug(writer.toString());
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }


  public OQueryResult getEntityTypeFromSingleTable(String discriminatorColumn, String physicalEntityName, String[] propertyOfKey, String[] valueOfKey, OTeleporterContext context) {
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
        context.getOutputManager().error(e.getMessage());
        Writer writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        context.getOutputManager().debug(writer.toString());
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
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
        context.getOutputManager().error(e.getMessage());
        Writer writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        context.getOutputManager().debug(writer.toString());
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    }catch(SQLException e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }
    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }

}
