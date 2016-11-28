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

package com.orientechnologies.teleporter.persistence.handler;

import com.orientechnologies.orient.core.metadata.schema.OType;

import java.util.HashMap;
import java.util.Map;

/**
 * Handler that executes type conversions from Oracle DBMS to the OrientDB types.
 * No Geospatial implemented.
 * 
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public class OOracleDataTypeHandler extends ODBMSDataTypeHandler {


  public OOracleDataTypeHandler(){
    this.dbmsType2OrientType = this.fillTypesMap();
    super.jsonImplemented = false;
    super.geospatialImplemented = false;
  }


  private Map<String, OType> fillTypesMap() {

    Map<String, OType> dbmsType2OrientType = new HashMap<String, OType>();

    /*
     * Character Types
     * (doc at http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT213 )
     */
    dbmsType2OrientType.put("char", OType.STRING);
    dbmsType2OrientType.put("varchar", OType.STRING);
    dbmsType2OrientType.put("varchar2", OType.STRING);
    dbmsType2OrientType.put("nvarchar", OType.STRING);
    dbmsType2OrientType.put("nvarchar2", OType.STRING);
    dbmsType2OrientType.put("clob", OType.STRING);
    dbmsType2OrientType.put("nclob", OType.STRING);
    dbmsType2OrientType.put("long", OType.STRING);


    /*
     * Numeric Types
     * (doc at http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT313 )
     */
    dbmsType2OrientType.put("number", OType.DOUBLE);
    dbmsType2OrientType.put("numeric", OType.DECIMAL);
    dbmsType2OrientType.put("float", OType.FLOAT);
    dbmsType2OrientType.put("binary_float", OType.FLOAT);
    dbmsType2OrientType.put("double", OType.DOUBLE);
    dbmsType2OrientType.put("binary_double", OType.DOUBLE);



    /*
     * Date/Time Types
     * (doc at http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT413 )
     */    
    dbmsType2OrientType.put("date", OType.DATE);
    dbmsType2OrientType.put("datetime", OType.DATETIME);
    dbmsType2OrientType.put("timestamp", OType.DATETIME);
    dbmsType2OrientType.put("timestamp with time zone", OType.DATETIME);
    dbmsType2OrientType.put("timestamp with local time zone", OType.DATETIME);


    /*
     * Binary Data Types
     * (doc at http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT613 )
     */
    dbmsType2OrientType.put("blob", OType.BINARY);
    dbmsType2OrientType.put("bfile", OType.BINARY);
    dbmsType2OrientType.put("raw", OType.BINARY);
    dbmsType2OrientType.put("long raw", OType.BINARY);


    /*
     * JSON Type
     * (doc at https://docs.oracle.com/database/121/ADXDB/json.htm#GUID-E6CC0DCF-3D72-41EF-ACA4-B3BF54EE3CA0__CACHFFCE)
     *
     * Unlike XML data, which is stored using SQL data type XMLType, JSON data is stored in Oracle Database using SQL data types VARCHAR2, CLOB, and BLOB.
     * Oracle recommends that you always use an is_json check constraint to ensure that column values are valid JSON instances
     */


    /*
     * ROWID and UROWID Data Types
     * (doc at http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT713 )
     */
    dbmsType2OrientType.put("rowid", OType.STRING);
    dbmsType2OrientType.put("urowid", OType.STRING);


    /*
     * ANSI, DB2, and SQL/DS Datatypes
     * (doc at http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT813)
     */
    // TODO !!!


    /*
     * XML Type
     * (doc at http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT913 )
     */
    dbmsType2OrientType.put("xmltype", OType.STRING);


    /*
     * URI Type
     * (doc at http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1856 )
     */
    dbmsType2OrientType.put("uritype", OType.STRING);


    /*
     * User Defined Types  (Object data types and object views)
     * (doc at http://www.postgresql.org/docs/9.3/static/rowtypes.html)
     */
    //    TODO! in EMBEDDED

    return dbmsType2OrientType;
  }

}
