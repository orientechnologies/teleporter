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

package com.orientechnologies.teleporter.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.persistence.util.ODBSourceConnection;

/**
 * Executes an automatic configuration of the chosen driver JDBC.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODriverConfigurator {

	public static final String        DRIVERS = "http://orientdb.com/jdbc-drivers.json";
	private Map<String,List<String>> driver2filesIdentifier;

	public ODriverConfigurator() {
		this.driver2filesIdentifier = new LinkedHashMap<String,List<String>>();
		this.fillMap();
	}

	/**
	 * 
	 */
	private void fillMap() {

		// Oracle identifiers
		List<String> oracleIdentifiers = new LinkedList<String>();
		oracleIdentifiers.add("ojdbc");
		driver2filesIdentifier.put("oracle", oracleIdentifiers);

		// SQLServer identifiers
		List<String> sqlserverIdentifiers = new LinkedList<String>();
		sqlserverIdentifiers.add("sqljdbc");
		sqlserverIdentifiers.add("jtds");
		driver2filesIdentifier.put("sqlserver", sqlserverIdentifiers);

		// MySQL identifiers
		List<String> mysqlIdentifiers = new LinkedList<String>();
		mysqlIdentifiers.add("mysql");
		driver2filesIdentifier.put("mysql", mysqlIdentifiers);

		// PostgreSQL identifiers
		List<String> postgresqlIdentifiers = new LinkedList<String>();
		postgresqlIdentifiers.add("postgresql");
		driver2filesIdentifier.put("postgresql", postgresqlIdentifiers);

		// HyperSQL identifiers
		List<String> hypersqlIdentifiers = new LinkedList<String>();
		hypersqlIdentifiers.add("hsqldb");
		driver2filesIdentifier.put("hypersql", hypersqlIdentifiers);


	}

	/*
	 * It Checks if the requested driver is already present in the classpath, if not present it downloads the last available driver version.
	 * Moreover it gets the driver class name corresponding to the chosen DBMS.
	 */
	public String checkConfiguration(String driverName, OTeleporterContext context) {

		String classPath = "../lib/";
		String driverClassName = null;
		driverName = driverName.toLowerCase();

		try {

			// fetching online JSON
			ODocument json = readJsonFromUrl("http://orientdb.com/jdbc-drivers.json", context);

			LinkedHashMap<String,String> fields = null;

			// recovering driver class name
			if(driverName.equals("oracle")) {
				fields = json.field("Oracle");
			}
			if(driverName.equals("sqlserver")) {
				fields = json.field("SQLServer");
			}
			else if(driverName.equals("mysql")) {
				fields = json.field("MySQL");
			}
			else if(driverName.equals("postgresql")) {
				fields = json.field("PostgreSQL");
			}
			else if(driverName.equals("hypersql")) {
				fields = json.field("HyperSQL");
			}
			driverClassName = (String) fields.get("className");

			// if the driver is not present, it will be downloaded
			String driverPath = isDriverAlreadyPresent(driverName, classPath);

			if(driverPath == null) {

				context.getOutputManager().info("Downloading the necessary JDBC driver in ORIENTDB_HOME/lib ...");

				// download last available jdbc driver version
				String driverDownldUrl = (String) fields.get("url");
				URL website = new URL(driverDownldUrl);
				String fileName = driverDownldUrl.substring(driverDownldUrl.lastIndexOf('/')+1, driverDownldUrl.length());
				ReadableByteChannel rbc = Channels.newChannel(website.openStream());
				@SuppressWarnings("resource")
				FileOutputStream fos = new FileOutputStream(classPath+fileName);
				fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

				driverPath = classPath+fileName;

				if(driverName.equalsIgnoreCase("SQLServer")) {
					OFileManager.extractAll(driverPath, classPath);
					OFileManager.deleteFile(driverPath);
					String[] split = driverPath.split(".jar");
					driverPath = split[0] + ".jar";
				}

				context.getOutputManager().info("Driver JDBC downloaded.");
			}

			// saving driver
			context.setDriverDependencyPath(driverPath);

		} catch(Exception e) {
			if(e.getMessage() != null)
				context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
			else
				context.getOutputManager().error(e.getClass().getName());

			Writer writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));
			String s = writer.toString();
			context.getOutputManager().debug("\n" + s + "\n");
			throw new OTeleporterRuntimeException();
		}

		return driverClassName;
	}


	/**
	 * @param driverName
	 * @return the path of the driver
	 */
	private String isDriverAlreadyPresent(String driverName, String classPath) {

		File dir = new File(classPath);
		File[] files = dir.listFiles(); 

		for(String identifier: this.driver2filesIdentifier.get(driverName)) {

			for(int i=0; i<files.length; i++) {
				if(files[i].getName().startsWith(identifier))
					return files[i].getPath();
			}
		}

		// the driver is not present, thus it's returned null as path
		return null;
	}

	private String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public ODocument readJsonFromUrl(String url, OTeleporterContext context) {

		InputStream is = null;
		ODocument json = null;

		try {

			URL urlObj = new URL(url);
			URLConnection urlConn = urlObj.openConnection();
			urlConn.setRequestProperty("User-Agent", "Teleporter");
			is = urlConn.getInputStream();

			json = new ODocument();

			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			json.fromJSON(jsonText);

		} catch (Exception e) {
			if(e.getMessage() != null)
				context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
			else
				context.getOutputManager().error(e.getClass().getName());

			Writer writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));
			String s = writer.toString();
			context.getOutputManager().debug("\n" + s + "\n");
			throw new OTeleporterRuntimeException();
		}
		finally {
			try {
				is.close();
			} catch (Exception e) {
				if(e.getMessage() != null)
					context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
				else
					context.getOutputManager().error(e.getClass().getName());

				Writer writer = new StringWriter();
				e.printStackTrace(new PrintWriter(writer));
				String s = writer.toString();
				context.getOutputManager().debug("\n" + s + "\n");
			}
		}
		return json;
	}

	public void checkConnection(String driver, String uri, String username, String password, OTeleporterContext context) throws Exception {

		String driverName = checkConfiguration(driver, context);

		ODBSourceConnection odbSourceConnection = new ODBSourceConnection(driverName, uri, username, password);
		Connection connection = null;
		try {
			connection = odbSourceConnection.getConnection(context);
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

}
