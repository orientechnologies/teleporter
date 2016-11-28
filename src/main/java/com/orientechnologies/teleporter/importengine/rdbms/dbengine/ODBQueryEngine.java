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
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
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
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public class ODBQueryEngine implements ODataSourceQueryEngine {

  private OQueryBuilderFactory queryBuilderFactory;
  private OQueryBuilder       queryBuilder;


  public ODBQueryEngine(String driver) {
    this.queryBuilderFactory = new OQueryBuilderFactory();
    this.queryBuilder = this.queryBuilderFactory.buildQueryBuilder(driver);
  }


  public OQueryResult countTableRecords(OSourceDatabaseInfo sourceDBInfo, String currentTableName, String currentTableSchema) {

    String query = queryBuilder.countTableRecords(currentTableName, currentTableSchema);
    return this.executeQuery(query, sourceDBInfo);
  }

  /**
   * @param entity
   * @param propertyOfKey
   * @param valueOfKey
   * @return
   */
  public OQueryResult getRecordById(OEntity entity, String[] propertyOfKey, String[] valueOfKey) {

    OSourceDatabaseInfo sourceDBInfo = entity.getSourceDataseInfo();
    // TODO: queryBuilder fetching
    String query = queryBuilder.getRecordById(entity, propertyOfKey, valueOfKey);
    return this.executeQuery(query, sourceDBInfo);
  }


  public OQueryResult getRecordsByEntity(OEntity entity) {

    OSourceDatabaseInfo sourceDBInfo = entity.getSourceDataseInfo();
    // TODO: queryBuilder fetching
    String query = queryBuilder.getRecordsByEntity(entity);
    return this.executeQuery(query, sourceDBInfo);
  }

  public OQueryResult getRecordsFromMultipleEntities(List<OEntity> mappedEntities, String[][] columns) {

    OSourceDatabaseInfo sourceDBInfo = mappedEntities.get(0).getSourceDataseInfo();   // all the entities belong to the same source database
    // TODO: queryBuilder fetching
    String query = queryBuilder.getRecordsFromMultipleEntities(mappedEntities, columns);
    return this.executeQuery(query, sourceDBInfo);
  }


  /**
   * @param currentDiscriminatorValue
   */
  public OQueryResult getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, OEntity entity) {

    OSourceDatabaseInfo sourceDBInfo = entity.getSourceDataseInfo();
    // TODO: queryBuilder fetching
    String query = queryBuilder.getRecordsFromSingleTableByDiscriminatorValue(discriminatorColumn, currentDiscriminatorValue, entity);
    return this.executeQuery(query, sourceDBInfo);
  }


  public OQueryResult getEntityTypeFromSingleTable(String discriminatorColumn, OEntity entity, String[] propertyOfKey, String[] valueOfKey) {

    OSourceDatabaseInfo sourceDBInfo = entity.getSourceDataseInfo();
    // TODO: queryBuilder fetching
    String query = queryBuilder.getEntityTypeFromSingleTable(discriminatorColumn, entity, propertyOfKey, valueOfKey);
    return this.executeQuery(query, sourceDBInfo);
  }


  /**
   * @param bag
   * @return
   */
  public OQueryResult buildAggregateTableFromHierarchicalBag(OHierarchicalBag bag) {

    OSourceDatabaseInfo sourceDBInfo = bag.getSourceDataseInfo();
    // TODO: queryBuilder fetching
    String query = queryBuilder.buildAggregateTableFromHierarchicalBag(bag);
    return this.executeQuery(query, sourceDBInfo);
  }

  public OQueryResult executeQuery(String query, OSourceDatabaseInfo sourceDBInfo) {

    ResultSet result = null;
    Connection dbConnection = null;
    Statement statement = null;

    try {
      try {
        dbConnection = ODBSourceConnection.getConnection(sourceDBInfo);
      } catch (Exception e) {
        String mess = "";
        OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
        OTeleporterContext.getInstance().printExceptionStackTrace(e, "debug");
      }
      statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      result = statement.executeQuery(query);

    } catch (SQLException e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "debug");
    }

    OQueryResult queryResult = new OQueryResult(dbConnection, statement, result);
    return queryResult;
  }

}
