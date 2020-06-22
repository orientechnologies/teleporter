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

import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.HashMap;
import java.util.Map;

/**
 * Handler that executes type conversions from HSQLDB DBMS to the OrientDB types. No Geospatial
 * implementable (HSQLDB doesn't support this feature).
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
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
