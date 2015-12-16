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

package com.orientdb.teleporter.persistence.handler;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Handler that executes type conversions from HSQLDB DBMS to the OrientDB types.
 * No Geospatial implementable (HSQLDB doesn't support this feature).
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OHSQLDBDataTypeHandler extends ODBMSDataTypeHandler {


  public OHSQLDBDataTypeHandler() {
    this.dbmsType2OrientType = this.fillTypesMap();
    super.jsonImplemented = false;
    super.geospatialImplemented = false;
  }


  private Map<String, OType> fillTypesMap() {

    Map<String, OType> dbmsType2OrientType = new HashMap<String, OType>();


    /*
     * Character Types
     * (doc at http://hsqldb.org/doc/guide/guide.html#sgc_char_types )
     */
    dbmsType2OrientType.put("character", OType.STRING);
    dbmsType2OrientType.put("character varying", OType.STRING);
    dbmsType2OrientType.put("clob", OType.STRING);
    dbmsType2OrientType.put("char", OType.STRING);
    dbmsType2OrientType.put("char varying", OType.STRING);
    dbmsType2OrientType.put("varchar", OType.STRING);
    dbmsType2OrientType.put("longvarchar", OType.STRING);
    dbmsType2OrientType.put("character large object", OType.STRING);


    /*
     * Numeric Types
     * (doc at http://hsqldb.org/doc/guide/guide.html#sgc_numeric_types )
     */
    dbmsType2OrientType.put("tinyint", OType.SHORT); 
    dbmsType2OrientType.put("smallint", OType.SHORT); 
    dbmsType2OrientType.put("integer", OType.INTEGER); 
    dbmsType2OrientType.put("bigint", OType.LONG);
    dbmsType2OrientType.put("real", OType.DOUBLE); 
    dbmsType2OrientType.put("float", OType.DOUBLE); 
    dbmsType2OrientType.put("double", OType.DOUBLE);
    dbmsType2OrientType.put("double precision", OType.DOUBLE);
    dbmsType2OrientType.put("numeric", OType.DECIMAL);
    dbmsType2OrientType.put("decimal", OType.DECIMAL);


    /*
     * Bit String Types
     * (doc at http://hsqldb.org/doc/guide/guide.html#sgc_bit_types )
     */
    dbmsType2OrientType.put("bit", OType.STRING);
    dbmsType2OrientType.put("bit varying", OType.STRING);



    /*
     * Date/Time Types
     * (doc at http://hsqldb.org/doc/guide/guide.html#sgc_datetime_types )
     */    
    dbmsType2OrientType.put("date", OType.DATE);
    dbmsType2OrientType.put("time", OType.STRING);
    dbmsType2OrientType.put("time with time zone", OType.STRING);
    dbmsType2OrientType.put("timestamp", OType.DATETIME);
    dbmsType2OrientType.put("timestamp with time zone", OType.DATETIME);


    /*
     * Boolean Type
     * (doc at http://hsqldb.org/doc/guide/guide.html#sgc_boolean_type )
     */
    dbmsType2OrientType.put("boolean", OType.BOOLEAN);


    /*
     * Binary Data Types
     * (doc at http://hsqldb.org/doc/guide/guide.html#sgc_binary_types )
     */
    dbmsType2OrientType.put("binary", OType.BINARY);
    dbmsType2OrientType.put("binary varying", OType.BINARY);
    dbmsType2OrientType.put("blob", OType.BINARY);
    dbmsType2OrientType.put("varbinary", OType.BINARY);
    dbmsType2OrientType.put("binary large object", OType.BINARY);
    dbmsType2OrientType.put("longvarbinary", OType.BINARY);


    /*
     * User Defined Types  (Object data types and object views)
     */
    //    TODO! in EMBEDDED


    return dbmsType2OrientType;
  }

}
