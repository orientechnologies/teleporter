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
 * Generic Handler that executes generic type conversions to the OrientDB types.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OGenericDataTypeHandler implements ODriverDataTypeHandler {

  private Map<String,OType> dbmsType2OrientType;

  public OGenericDataTypeHandler(){
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
     */
    dbmsType2OrientType.put("text", OType.STRING);
    dbmsType2OrientType.put("character", OType.STRING);
    dbmsType2OrientType.put("character varying", OType.STRING);
    dbmsType2OrientType.put("char", OType.STRING);
    dbmsType2OrientType.put("varchar", OType.STRING);
    dbmsType2OrientType.put("varchar2", OType.STRING);


    /*
     * Numeric Types
     */
    dbmsType2OrientType.put("int2", OType.SHORT); 
    dbmsType2OrientType.put("int", OType.INTEGER); 
    dbmsType2OrientType.put("integer", OType.INTEGER); 
    dbmsType2OrientType.put("int4", OType.INTEGER);
    dbmsType2OrientType.put("int8", OType.LONG);
    dbmsType2OrientType.put("real", OType.LONG); 
    dbmsType2OrientType.put("float", OType.LONG); 
    dbmsType2OrientType.put("float4", OType.LONG);
    dbmsType2OrientType.put("float8", OType.DOUBLE);
    dbmsType2OrientType.put("double", OType.DOUBLE);
    dbmsType2OrientType.put("double precision", OType.DOUBLE);
    dbmsType2OrientType.put("numeric", OType.DECIMAL);
    dbmsType2OrientType.put("decimaln", OType.DECIMAL);




    /*
     * Date/Time Types
     */    
    dbmsType2OrientType.put("date", OType.DATETIME);
    dbmsType2OrientType.put("datetime", OType.DATETIME);
    dbmsType2OrientType.put("timestamp", OType.DATETIME);
    dbmsType2OrientType.put("timestamp with time zone", OType.DATETIME);
    dbmsType2OrientType.put("timestamp with local time zone", OType.DATETIME);
    
    
    
    /*
     * Boolean Type
     */
    dbmsType2OrientType.put("boolean", OType.BOOLEAN);
    dbmsType2OrientType.put("bool", OType.BOOLEAN);


    /*
     * Binary Data Types
     */
    dbmsType2OrientType.put("blob", OType.BINARY);



    /*
     * User Defined Types  (Object data types and object views)
     */
    //    TODO! in EMBEDDED



    return dbmsType2OrientType;
  }

}
