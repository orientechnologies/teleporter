/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
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
 * @email <g.ponzi--at--orientdb.com>
 */

public class ODBQueryEngine implements ODataSourceQueryEngine {

  private OQueryBuilderFactory queryBuilderFactory;
  private OQueryBuilder        queryBuilder;

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
   *
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

    OSourceDatabaseInfo sourceDBInfo = mappedEntities.get(0)
        .getSourceDataseInfo();   // all the entities belong to the same source database
    // TODO: queryBuilder fetching
    String query = queryBuilder.getRecordsFromMultipleEntities(mappedEntities, columns);
    return this.executeQuery(query, sourceDBInfo);
  }

  /**
   * @param currentDiscriminatorValue
   */
  public OQueryResult getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue,
      OEntity entity) {

    OSourceDatabaseInfo sourceDBInfo = entity.getSourceDataseInfo();
    // TODO: queryBuilder fetching
    String query = queryBuilder
        .getRecordsFromSingleTableByDiscriminatorValue(discriminatorColumn, currentDiscriminatorValue, entity);
    return this.executeQuery(query, sourceDBInfo);
  }

  public OQueryResult getEntityTypeFromSingleTable(String discriminatorColumn, OEntity entity, String[] propertyOfKey,
      String[] valueOfKey) {

    OSourceDatabaseInfo sourceDBInfo = entity.getSourceDataseInfo();
    // TODO: queryBuilder fetching
    String query = queryBuilder.getEntityTypeFromSingleTable(discriminatorColumn, entity, propertyOfKey, valueOfKey);
    return this.executeQuery(query, sourceDBInfo);
  }

  /**
   * @param bag
   *
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
