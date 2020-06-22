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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler that executes type conversions from PostgreSQL DBMS to the OrientDB types. Geospatial
 * implemented (PostGis).
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OPostgreSQLDataTypeHandler extends ODBMSDataTypeHandler {

  private List<String> geospatialTypes;

  public OPostgreSQLDataTypeHandler() {
    this.dbmsType2OrientType = this.fillTypesMap();
    this.geospatialTypes = this.fillGeospatialList();
    super.jsonImplemented = true;
    super.geospatialImplemented = false;
  }

  private Map<String, OType> fillTypesMap() {

    Map<String, OType> dbmsType2OrientType = new HashMap<String, OType>();

    /*
     * Numeric Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-numeric.html )
     */
    dbmsType2OrientType.put("smallint", OType.SHORT);
    dbmsType2OrientType.put("int2", OType.SHORT);
    dbmsType2OrientType.put("integer", OType.INTEGER);
    dbmsType2OrientType.put("int", OType.INTEGER);
    dbmsType2OrientType.put("int4", OType.INTEGER);
    dbmsType2OrientType.put("bigint", OType.LONG);
    dbmsType2OrientType.put("int8", OType.LONG);
    dbmsType2OrientType.put("decimal", OType.DECIMAL);
    dbmsType2OrientType.put("numeric", OType.DECIMAL);
    dbmsType2OrientType.put("real", OType.FLOAT);
    dbmsType2OrientType.put("float4", OType.FLOAT);
    dbmsType2OrientType.put("double precision", OType.DOUBLE);
    dbmsType2OrientType.put("float8", OType.DOUBLE);
    dbmsType2OrientType.put("smallserial", OType.SHORT);
    dbmsType2OrientType.put("serial2", OType.SHORT);
    dbmsType2OrientType.put("serial", OType.INTEGER);
    dbmsType2OrientType.put("serial4", OType.INTEGER);
    dbmsType2OrientType.put("bigserial", OType.LONG);
    dbmsType2OrientType.put("serial8", OType.LONG);

    /*
     * Monetary Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-money.html )
     */
    dbmsType2OrientType.put("money", OType.DOUBLE);

    /*
     * Character Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-character.html )
     */
    dbmsType2OrientType.put("character varying", OType.STRING);
    dbmsType2OrientType.put("varchar", OType.STRING);
    dbmsType2OrientType.put("character", OType.STRING);
    dbmsType2OrientType.put("char", OType.STRING);
    dbmsType2OrientType.put("text", OType.STRING);

    /*
     * Binary Data Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-binary.html )
     */
    dbmsType2OrientType.put("bytea", OType.BINARY);

    /*
     * Date/Time Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-datetime.html )
     */
    dbmsType2OrientType.put("timestamp", OType.DATETIME);
    dbmsType2OrientType.put("date", OType.DATE);
    dbmsType2OrientType.put("time", OType.STRING);
    dbmsType2OrientType.put("interval", OType.STRING);

    /*
     * Boolean Type
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-boolean.html )
     */
    dbmsType2OrientType.put("boolean", OType.BOOLEAN);
    dbmsType2OrientType.put("bool", OType.BOOLEAN);

    /*
     *  Enumerated Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-enum.html )
     */
    // TODO?!

    /*
     * Geometric Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-geometric.html )
     */
    dbmsType2OrientType.put("point", OType.STRING);
    dbmsType2OrientType.put("line", OType.STRING);
    dbmsType2OrientType.put("lseg", OType.STRING);
    dbmsType2OrientType.put("box", OType.STRING);
    dbmsType2OrientType.put("path", OType.STRING);
    dbmsType2OrientType.put("polygon", OType.STRING);
    dbmsType2OrientType.put("circle", OType.STRING);
    // Postgis
    dbmsType2OrientType.put("box2d", OType.EMBEDDED);
    dbmsType2OrientType.put("box3d", OType.EMBEDDED);
    dbmsType2OrientType.put("geometry", OType.EMBEDDED);
    dbmsType2OrientType.put("geometry_dump", OType.EMBEDDED);
    dbmsType2OrientType.put("geography", OType.EMBEDDED);

    /*
     * Network Address Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-net-types.html )
     */
    dbmsType2OrientType.put("cidr", OType.STRING);
    dbmsType2OrientType.put("inet", OType.STRING);
    dbmsType2OrientType.put("macaddr", OType.STRING);

    /*
     * Bit String Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-bit.html )
     */
    dbmsType2OrientType.put("bit", OType.STRING);
    dbmsType2OrientType.put("bit varying", OType.STRING);
    dbmsType2OrientType.put("varbit", OType.STRING);

    /*
     * Text Search Types
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-textsearch.html )
     */
    // TODO

    /*
     * UUID Type
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-uuid.html )
     */
    dbmsType2OrientType.put("uuid", OType.STRING);

    /*
     * XML Type
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-xml.html )
     */
    dbmsType2OrientType.put("xml", OType.STRING);

    /*
     * JSON Type
     * (doc at http://www.postgresql.org/docs/9.6/static/datatype-json.html )
     */
    dbmsType2OrientType.put("json", OType.EMBEDDED);
    dbmsType2OrientType.put("jsonb", OType.EMBEDDED);

    /*
     * Composite Types
     * (doc at http://www.postgresql.org/docs/9.6/static/rowtypes.html )
     */
    //    TODO! in EMBEDDED

    /*
     *  Range Types
     *  (doc at http://www.postgresql.org/docs/9.6/static/rangetypes.html )
     */
    dbmsType2OrientType.put("int4range", OType.STRING);
    dbmsType2OrientType.put("int8range", OType.STRING);
    dbmsType2OrientType.put("numrange", OType.STRING);
    dbmsType2OrientType.put("tsrange", OType.STRING);
    dbmsType2OrientType.put("tstzrange", OType.STRING);
    dbmsType2OrientType.put("daterange", OType.STRING);

    return dbmsType2OrientType;
  }

  private List<String> fillGeospatialList() {

    List<String> geospatialTypes = new ArrayList<String>();

    geospatialTypes.add("box2d");
    geospatialTypes.add("box3d");
    geospatialTypes.add("geometry");
    geospatialTypes.add("geometry_dump");
    geospatialTypes.add("geography");

    return geospatialTypes;
  }
}
