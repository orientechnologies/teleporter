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

package com.orientechnologies.orient.teleporter.persistence.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.orientechnologies.orient.teleporter.context.OTeleporterContext;

/**
 * Encapsulates the query results with the correspondent statement and connection.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
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



  public void closeAll(OTeleporterContext context) {

    try {
      if(this.statement != null && !this.statement.isClosed()) 
        this.statement.close();
      if(this.result != null && !this.result.isClosed())
        this.result.close();
      if(this.dbConnection != null && !this.dbConnection.isClosed()) 
        this.dbConnection.close();
    } catch(SQLException e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }
  }

  public boolean isConnectionClosed(OTeleporterContext context) {
    try {
      if(this.dbConnection != null)
        return dbConnection.isClosed();
    }catch(Exception e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }
    return false;
  }
  
  public boolean isStatementClosed(OTeleporterContext context) {
    try {
      if(this.statement != null)
        return statement.isClosed();
    }catch(Exception e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }
    return false;
  }
  
  public boolean isResultSetClosed(OTeleporterContext context) {
    try {
      if(this.result != null)
        return result.isClosed();
    }catch(Exception e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }
    return false;
  }

  public boolean isAllClosed(OTeleporterContext context) {

    if(isConnectionClosed(context) && isStatementClosed(context) && isResultSetClosed(context))
      return true;
    else
      return false;
  }

}
