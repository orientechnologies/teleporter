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
  private Statement  statement;
  private ResultSet  result;

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
      if (this.statement != null && !this.statement.isClosed())
        this.statement.close();
      if (this.result != null && !this.result.isClosed())
        this.result.close();
      if (this.dbConnection != null && !this.dbConnection.isClosed())
        this.dbConnection.close();
    } catch (SQLException e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "debug");
    }
  }

  public boolean isConnectionClosed(OTeleporterContext context) {
    try {
      if (this.dbConnection != null)
        return dbConnection.isClosed();
    } catch (Exception e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    return false;
  }

  public boolean isStatementClosed(OTeleporterContext context) {
    try {
      if (this.statement != null)
        return statement.isClosed();
    } catch (Exception e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    return false;
  }

  public boolean isResultSetClosed(OTeleporterContext context) {
    try {
      if (this.result != null)
        return result.isClosed();
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
    else
      return false;
  }

}
