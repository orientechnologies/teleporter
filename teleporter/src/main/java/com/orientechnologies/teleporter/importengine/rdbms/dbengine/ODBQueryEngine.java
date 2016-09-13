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

package com.orientechnologies.teleporter.importengine.rdbms.dbengine;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.factory.OQueryBuilderFactory;
import com.orientechnologies.teleporter.importengine.ODataSourceQueryEngine;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;
import com.orientechnologies.teleporter.persistence.util.ODBSourceConnection;
import com.orientechnologies.teleporter.persistence.util.OQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Implementation of ODataSourceQueryEngine. It executes the necessary queries for the source DB records fetching.
 *
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODBQueryEngine implements ODataSourceQueryEngine {

  private ODBSourceConnection dataSource;
  private OQueryBuilderFactory queryBuilderFactory;
  private OQueryBuilder       queryBuilder;


  public ODBQueryEngine(String driver, String uri, String username, String password, OTeleporterContext context) {
    this.dataSource =  new ODBSourceConnection(driver, uri, username, password);
    this.queryBuilderFactory = new OQueryBuilderFactory();
    this.queryBuilder = this.queryBuilderFactory.buildQueryBuilder(driver, context);
  }


  public OQueryResult countTableRecords(String currentTableName, String currentTableSchema, OTeleporterContext context) {

    String query = queryBuilder.countTableRecords(currentTableName, currentTableSchema, context);
    return this.executeQuery(query, context);
  }

  /**
   * @param entity
   * @param propertyOfKey
   * @param valueOfKey
   * @param context
   * @return
   */
  public OQueryResult getRecordById(OEntity entity, String[] propertyOfKey, String[] valueOfKey, OTeleporterContext context) {

    String query = queryBuilder.getRecordById(entity, propertyOfKey, valueOfKey, context);
    return this.executeQuery(query, context);
  }


  public OQueryResult getRecordsByEntity(OEntity entity, OTeleporterContext context) {

    String query = queryBuilder.getRecordsByEntity(entity, context);
    return this.executeQuery(query, context);
  }

  public OQueryResult getRecordsFromMultipleEntities(List<OEntity> mappedEntities, String[][] columns, OTeleporterContext context) {

    String query = queryBuilder.getRecordsFromMultipleEntities(mappedEntities, columns, context);
    return this.executeQuery(query, context);
  }


  /**
   * @param currentDiscriminatorValue
   */
  public OQueryResult getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, OEntity entity, OTeleporterContext context) {

    String query = queryBuilder.getRecordsFromSingleTableByDiscriminatorValue(discriminatorColumn, currentDiscriminatorValue, entity, context);
    return this.executeQuery(query, context);
  }


  public OQueryResult getEntityTypeFromSingleTable(String discriminatorColumn, OEntity entity, String[] propertyOfKey, String[] valueOfKey, OTeleporterContext context) {

    String query = queryBuilder.getEntityTypeFromSingleTable(discriminatorColumn, entity, propertyOfKey, valueOfKey, context);
    return this.executeQuery(query, context);
  }


  /**
   * @param bag
   * @return
   */
  public OQueryResult buildAggregateTableFromHierarchicalBag(OHierarchicalBag bag, OTeleporterContext context) {

    String query = queryBuilder.buildAggregateTableFromHierarchicalBag(bag, context);
    return this.executeQuery(query, context);
  }

  public OQueryResult executeQuery(String query, OTeleporterContext context) {

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

}
