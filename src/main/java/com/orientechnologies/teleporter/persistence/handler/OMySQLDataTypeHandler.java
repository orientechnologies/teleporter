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
 * Handler that executes type conversions from MySQL DBMS to the OrientDB types. No Geospatial
 * implemented.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OMySQLDataTypeHandler extends ODBMSDataTypeHandler {

  public OMySQLDataTypeHandler() {
    this.dbmsType2OrientType = this.fillTypesMap();
    super.jsonImplemented = true;
    super.geospatialImplemented = false;
  }

  private Map<String, OType> fillTypesMap() {

    Map<String, OType> dbmsType2OrientType = new HashMap<String, OType>();

    /*
     * Numeric Types
     * (doc at http://dev.mysql.com/doc/refman/5.7/en/numeric-types.html )
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
     * (doc at http://dev.mysql.com/doc/refman/5.7/en/numeric-types.html )
     */
    dbmsType2OrientType.put("bit", OType.STRING);

    /*
     * Date/Time Types
     * (doc at http://dev.mysql.com/doc/refman/5.7/en/date-and-time-types.html )
     */
    dbmsType2OrientType.put("date", OType.DATE);
    dbmsType2OrientType.put("datetime", OType.DATETIME);
    dbmsType2OrientType.put("timestamp", OType.DATETIME);
    dbmsType2OrientType.put("time", OType.STRING);
    dbmsType2OrientType.put("year", OType.STRING);

    /*
     * Character Types
     * (doc at http://dev.mysql.com/doc/refman/5.7/en/string-types.html )
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
     * (doc at http://dev.mysql.com/doc/refman/5.7/en/binary-varbinary.html
     *         http://dev.mysql.com/doc/refman/5.7/en/blob.html )
     */
    dbmsType2OrientType.put("binary", OType.BINARY);
    dbmsType2OrientType.put("varbinary", OType.BINARY);
    dbmsType2OrientType.put("tinyblob", OType.BINARY);
    dbmsType2OrientType.put("blob", OType.BINARY);
    dbmsType2OrientType.put("mediumblob", OType.BINARY);
    dbmsType2OrientType.put("longblob", OType.BINARY);

    /*
     * JSON Type
     * (doc at http://dev.mysql.com/doc/refman/5.7/en/json.html )
     */
    dbmsType2OrientType.put("json", OType.EMBEDDED);

    /*
     * ENUM Types
     * (doc at http://dev.mysql.com/doc/refman/5.7/en/enum.html )
     */
    // TODO !!!

    /*
     * SET Types
     * (doc at http://dev.mysql.com/doc/refman/5.7/en/set.html )
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
     * (doc at http://dev.mysql.com/doc/refman/5.7/en/other-vendor-data-types.html )
     */
    // TODO

    return dbmsType2OrientType;
  }
}
