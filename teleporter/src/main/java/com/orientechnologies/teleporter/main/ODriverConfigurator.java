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

package com.orientechnologies.teleporter.main;

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
import java.util.LinkedHashMap;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OTeleporterContext;

/**
 * Executes an automatic configuration of the chosen driver JDBC.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODriverConfigurator {


	/*
	 * It Checks if the requested driver is already present in the classpath, if not present it downloads the last available driver version.
	 */
	public static String checkConfiguration(final String driverName, OTeleporterContext context) {

		String classPath = "../lib/";
		String driverClassName = null;

		try {

			// fetching online JSON
			ODocument json = readJsonFromUrl("http://orientdb.com/jdbc-drivers.json", context);

			LinkedHashMap<String,String> fields = null;

			// recovering driver class name
			if(driverName.equalsIgnoreCase("Oracle")) {
				fields = json.field("Oracle");
			}
			else if(driverName.equalsIgnoreCase("MySQL")) {
				fields = json.field("MySQL");
			}
			else if(driverName.equalsIgnoreCase("PostgreSQL")) {
				fields = json.field("PostgreSQL");
			}
			else if(driverName.equalsIgnoreCase("HyperSQL")) {
				fields = json.field("HyperSQL");
			}
			driverClassName = (String) fields.get("className");

			// if the driver is not present, it will be downloaded
			String driverPath = isDriverAlreadyPresent(driverName, classPath);

			if(driverPath == null) {

				// download last available jdbc driver version
				String driverDownldUrl = (String) fields.get("url");
				URL website = new URL(driverDownldUrl);
				String fileName = driverDownldUrl.substring(driverDownldUrl.lastIndexOf('/')+1, driverDownldUrl.length());
				ReadableByteChannel rbc = Channels.newChannel(website.openStream());
				@SuppressWarnings("resource")
				FileOutputStream fos = new FileOutputStream(classPath+fileName);
				fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

				driverPath = classPath+fileName;
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
			System.exit(0);
		}

		return driverClassName;
	}


	/**
	 * @param driverName
	 * @return
	 */
	private static String isDriverAlreadyPresent(String driverName, String classPath) {

		// renaming prefix if referred to Oracle driver
		if(driverName.equalsIgnoreCase("Oracle"))
			driverName = "ojdbc";

		File dir = new File(classPath);

		File[] files = dir.listFiles(); 

		for(int i=0; i<files.length; i++) {
			if(files[i].getName().startsWith(driverName))
				return files[i].getPath();
		}

		return null;
	}

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public static ODocument readJsonFromUrl(String url, OTeleporterContext context) {

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
			System.exit(0);
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

}
