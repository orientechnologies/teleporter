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

package com.orientechnologies.teleporter.persistence.handler;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Handler that executes type conversions from MySQL DBMS to the OrientDB types.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OMySQLDataTypeHandler extends OGenericDataTypeHandler {


	public OMySQLDataTypeHandler(){
		this.dbmsType2OrientType = this.fillTypesMap();
	}


	private Map<String, OType> fillTypesMap() {

		Map<String, OType> dbmsType2OrientType = new HashMap<String, OType>();


		/*
		 * Numeric Types
		 * (doc at http://dev.mysql.com/doc/refman/5.6/en/numeric-types.html )
		 */
		dbmsType2OrientType.put("tinyint", OType.SHORT);
		dbmsType2OrientType.put("smallint", OType.SHORT);
		dbmsType2OrientType.put("mediumint", OType.INTEGER);
		dbmsType2OrientType.put("int", OType.INTEGER);
		dbmsType2OrientType.put("integer", OType.INTEGER);
		dbmsType2OrientType.put("bigint", OType.LONG);
		dbmsType2OrientType.put("decimal", OType.DECIMAL);
		dbmsType2OrientType.put("dec", OType.DECIMAL);
		dbmsType2OrientType.put("fixed", OType.DECIMAL);
		dbmsType2OrientType.put("numeric", OType.DECIMAL);
		dbmsType2OrientType.put("real", OType.FLOAT);
		dbmsType2OrientType.put("float", OType.FLOAT);
		dbmsType2OrientType.put("double", OType.DOUBLE);
		dbmsType2OrientType.put("double precision", OType.DOUBLE);


		/*
		 * Bit String Types
		 * (doc at http://dev.mysql.com/doc/refman/5.6/en/numeric-types.html )
		 */
		dbmsType2OrientType.put("bit", OType.STRING);


		/*
		 * Date/Time Types
		 * (doc at http://dev.mysql.com/doc/refman/5.6/en/date-and-time-types.html )
		 */    
		dbmsType2OrientType.put("date", OType.DATE);
		dbmsType2OrientType.put("datetime", OType.DATETIME);
		dbmsType2OrientType.put("timestamp", OType.DATETIME);
		dbmsType2OrientType.put("time", OType.STRING);
		dbmsType2OrientType.put("year", OType.STRING);


		/*
		 * Character Types
		 * (doc at http://dev.mysql.com/doc/refman/5.6/en/string-types.html )
		 */
		dbmsType2OrientType.put("char", OType.STRING);
		dbmsType2OrientType.put("varchar", OType.STRING);
		dbmsType2OrientType.put("binary", OType.STRING);
		dbmsType2OrientType.put("varbinary", OType.STRING);
		dbmsType2OrientType.put("tinytext", OType.STRING);
		dbmsType2OrientType.put("text", OType.STRING);
		dbmsType2OrientType.put("mediumtext", OType.STRING);
		dbmsType2OrientType.put("longtext", OType.STRING);


		/*
		 * Binary Data Types
		 * (doc at http://www.postgresql.org/docs/9.3/static/datatype-binary.html )
		 */    
		dbmsType2OrientType.put("tinyblob", OType.BINARY);
		dbmsType2OrientType.put("blob", OType.BINARY);
		dbmsType2OrientType.put("mediumblob", OType.BINARY);
		dbmsType2OrientType.put("longblob", OType.BINARY);


		/*
		 * ENUM Types
		 * (doc at http://dev.mysql.com/doc/refman/5.6/en/enum.html )
		 */
		// TODO !!!


		/*
		 * SET Types
		 * (doc at http://dev.mysql.com/doc/refman/5.6/en/set.html )
		 */
		// TODO !!!


		/*
		 * Geometric Types
		 * (doc at http://www.postgresql.org/docs/9.3/static/datatype-geometric.html )
		 */
		dbmsType2OrientType.put("geometry", OType.STRING);
		dbmsType2OrientType.put("point", OType.STRING);
		dbmsType2OrientType.put("linestring", OType.STRING);
		dbmsType2OrientType.put("line", OType.STRING);
		dbmsType2OrientType.put("linearRing", OType.STRING);
		dbmsType2OrientType.put("polygon", OType.STRING);
		dbmsType2OrientType.put("geometrycollection", OType.STRING);
		dbmsType2OrientType.put("multipoint", OType.STRING);
		dbmsType2OrientType.put("multilinestring", OType.STRING);
		dbmsType2OrientType.put("multipolygon", OType.STRING);


		/*
		 * Using Data Types from Other Database Engines
		 * (doc at http://dev.mysql.com/doc/refman/5.6/en/other-vendor-data-types.html )
		 */
		// TODO

		return dbmsType2OrientType;
	}

}
