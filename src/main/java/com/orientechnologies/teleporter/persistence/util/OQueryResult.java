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

package com.orientechnologies.teleporter.persistence.util;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Encapsulates the query results with the correspondent statement and connection.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OQueryResult {

  private Connection dbConnection;
  private Statement statement;
  private ResultSet result;

  public OQueryResult(Connection connection, Statement statement, ResultSet result) {
    this.dbConnection = connection;
    this.statement = statement;
    this.result = result;
  }

  public Connection getDbConnection() {
    return dbConnection;
  }

  public void setDbConnection(Connection dbConnection) {
    this.dbConnection = dbConnection;
  }

  public Statement getStatement() {
    return statement;
  }

  public void setStatement(Statement statement) {
    this.statement = statement;
  }

  public ResultSet getResult() {
    return result;
  }

  public void setResult(ResultSet result) {
    this.result = result;
  }

  public void closeAll() {

    try {
      if (this.statement != null && !this.statement.isClosed()) this.statement.close();
      if (this.result != null && !this.result.isClosed()) this.result.close();
      if (this.dbConnection != null && !this.dbConnection.isClosed()) this.dbConnection.close();
    } catch (SQLException e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "debug");
    }
  }

  public boolean isConnectionClosed(OTeleporterContext context) {
    try {
      if (this.dbConnection != null) return dbConnection.isClosed();
    } catch (Exception e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    return false;
  }

  public boolean isStatementClosed(OTeleporterContext context) {
    try {
      if (this.statement != null) return statement.isClosed();
    } catch (Exception e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    return false;
  }

  public boolean isResultSetClosed(OTeleporterContext context) {
    try {
      if (this.result != null) return result.isClosed();
    } catch (Exception e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    return false;
  }

  public boolean isAllClosed(OTeleporterContext context) {

    if (isConnectionClosed(context) && isStatementClosed(context) && isResultSetClosed(context))
      return true;
    else return false;
  }
}
