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

package com.orientechnologies.orient.drakkar.persistence.handler;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.drakkar.context.ODrakkarContext;

/**
 * Handler that executes type conversions from Oracle DBMS to the OrientDB types.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OOracleDataTypeHandler implements ODriverDataTypeHandler {

  private Map<String,OType> dbmsType2OrientType;

  public OOracleDataTypeHandler(){
    this.dbmsType2OrientType = fillTypesMap();
  }

  /**  
   * The method returns the Orient Type starting from the string name type of the Oracle DBMS.
   * If the starting type is not mapped, OType.STRING is returned.
   */
  public OType resolveType(String type, ODrakkarContext context) {

    // Defined Types
    if(this.dbmsType2OrientType.keySet().contains(type))
      return this.dbmsType2OrientType.get(type);

    // Undefined Types
    else {
      context.getStatistics().warningMessages.add("The original type '" + type + "' is not convertible into any Orient type thus, in order to prevent data loss, it will be converted to the Orient Type String.");
      return OType.STRING;
    }

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
    dbmsType2OrientType.put("numeric", OType.DECIMAL); 
    dbmsType2OrientType.put("binary_float", OType.FLOAT);
    dbmsType2OrientType.put("binary_double", OType.DOUBLE);


    /*
     * Date/Time Types
     * (doc at http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT413 )
     */    
    dbmsType2OrientType.put("date", OType.DATETIME);
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
