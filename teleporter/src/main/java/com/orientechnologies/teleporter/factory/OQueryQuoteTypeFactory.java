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

package com.orientechnologies.teleporter.factory;

import com.orientechnologies.teleporter.context.OTeleporterContext;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OQueryQuoteTypeFactory {

	public void buildQueryQuoteType(String driver, OTeleporterContext context) {
	    String queryQuoteType;

	    switch(driver) {

	    case "oracle.jdbc.driver.OracleDriver": queryQuoteType = "\"";
	    break;
	    
	    case "com.microsoft.sqlserver.jdbc.SQLServerDriver": queryQuoteType = "\"";
	    break;

	    case "com.mysql.jdbc.Driver":   queryQuoteType = "`";
	    break;

	    case "org.postgresql.Driver":   queryQuoteType = "\"";
	    break;
	    
	    case "org.hsqldb.jdbc.JDBCDriver": queryQuoteType = "\"";
	    break;

	    default :  queryQuoteType = "";
	    context.getStatistics().warningMessages.add("Driver " + driver + " is not completely supported, the \" quote will be adopted for the case-sensitive queries. "
	    		+ "Thus problems may occur during the querying.");
	    break;
	    }
	    
	    context.setQueryQuoteType(queryQuoteType);

	  }
	
	
}
