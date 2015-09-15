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
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;

import com.orientechnologies.teleporter.context.OTeleporterContext;

/**
 * Utility class to which connection with source DB is delegated.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ODBSourceConnection {

	private String driver;
	private String uri;
	private String username;
	private String password;


	public ODBSourceConnection(String driver, String uri, String username, String password) {	
		this.driver = driver;
		this.uri = uri;
		this.username = username;
		this.password = password;

	}

	public Connection getConnection(OTeleporterContext context) {

		Connection connection = null;
		
		try {
			URL u = new URL("jar:file:" + context.getDriverDependencyPath() + "!/");
			URLClassLoader ucl = new URLClassLoader(new URL[] { u });
			Driver d = (Driver) Class.forName(this.driver, true, ucl).newInstance();
			DriverManager.registerDriver(new ODriverShim(d));
			connection = DriverManager.getConnection(uri, username, password);

		} catch(Exception e) {
			if(e.getMessage() != null)
				context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
			else
				context.getOutputManager().error(e.getClass().getName());

			Writer writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));
			context.getOutputManager().debug(writer.toString());
			System.exit(0);
		}
		return connection;

	}

	public String getDriver() {
		return driver;
	}

	public String getUri() {
		return uri;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}
}