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

package com.orientechnologies.teleporter.persistence.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.orientechnologies.teleporter.context.OTeleporterContext;

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
			if(e.getMessage() != null)
				context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
			else
				context.getOutputManager().error(e.getClass().getName());

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
			if(e.getMessage() != null)
				context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
			else
				context.getOutputManager().error(e.getClass().getName());

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
			if(e.getMessage() != null)
				context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
			else
				context.getOutputManager().error(e.getClass().getName());

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
			if(e.getMessage() != null)
				context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
			else
				context.getOutputManager().error(e.getClass().getName());

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
