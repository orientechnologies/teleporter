/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.teleporter.persistence.handler;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Generic Handler that executes generic type conversions to the OrientDB types.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class ODBMSDataTypeHandler implements ODriverDataTypeHandler {

  protected Map<String, OType> dbmsType2OrientType;
  public    boolean            jsonImplemented;
  public    boolean            geospatialImplemented;

  public ODBMSDataTypeHandler() {
    this.dbmsType2OrientType = this.fillTypesMap();
    this.jsonImplemented = false;
    this.geospatialImplemented = false;
  }

  /**
   * The method returns the Orient Type starting from the string name type of the original DBMS.
   * If the starting type is not mapped, OType.STRING is returned.
   */
  public OType resolveType(String type) {

    // normalization
    type = type.toLowerCase(Locale.ENGLISH);

    // Defined Types
    if (this.dbmsType2OrientType.keySet().contains(type))
      return this.dbmsType2OrientType.get(type);

      // Undefined Types
    else {
      OTeleporterContext.getInstance().getStatistics().warningMessages.add("The original type '" + type
          + "' is not convertible into any OrientDB type thus, in order to prevent data loss, it will be converted to the OrientDB Type String.");
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
    dbmsType2OrientType.put("decimal", OType.DECIMAL);


    /*
     * Date/Time Types
     */
    dbmsType2OrientType.put("date", OType.DATE);
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

  public ODocument convertJSONToDocument(String currentProperty, String currentAttributeValue) {
    ODocument document = new ODocument(currentProperty);
    if (currentAttributeValue != null && currentAttributeValue.length() > 0) {
      document.fromJSON(currentAttributeValue, "noMap");
    }
    return document;
  }

  /**
   * @param currentOriginalType
   *
   * @return
   */
  public boolean isGeospatial(String currentOriginalType) {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * @param entity
   * @param context
   *
   * @return
   */
  public String buildGeospatialQuery(OEntity entity, OTeleporterContext context) {
    // TODO Auto-generated method stub
    return null;
  }

}
