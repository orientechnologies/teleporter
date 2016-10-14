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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Handler that executes type conversions from SQLServer DBMS to the OrientDB types.
 * No Geospatial implemented.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OSQLServerDataTypeHandler extends ODBMSDataTypeHandler {


  public OSQLServerDataTypeHandler(){
    this.dbmsType2OrientType = this.fillTypesMap();
    super.jsonImplemented = false;
    super.geospatialImplemented = false;
  }

  @Override
  public OType resolveType(String type) {

    // dropping "identity" sqlserver property
    type = type.replace("identity", "").trim();

    // Defined Types
    if(this.dbmsType2OrientType.keySet().contains(type))
      return this.dbmsType2OrientType.get(type);

    // Undefined Types
    else {
      OTeleporterContext.getInstance().getStatistics()
              .warningMessages.add("The original type '" + type + "' is not convertible into any OrientDB type thus, in order to prevent data loss, " +
              "it will be converted to the OrientDB Type String.");
      return OType.STRING;
    }
  }


  private Map<String, OType> fillTypesMap() {

    Map<String, OType> dbmsType2OrientType = new HashMap<String, OType>();

    /*
     * Character Types
     * (doc at https://msdn.microsoft.com/en-us/library/ms176089.aspx, https://msdn.microsoft.com/en-us/library/ms187993.aspx,
     * https://msdn.microsoft.com/en-us/library/ms186939.aspx )
     */
    dbmsType2OrientType.put("char", OType.STRING);
    dbmsType2OrientType.put("character", OType.STRING);
    dbmsType2OrientType.put("varchar", OType.STRING);
    dbmsType2OrientType.put("char varying", OType.STRING);
    dbmsType2OrientType.put("character varying", OType.STRING);
    dbmsType2OrientType.put("text", OType.STRING);
    dbmsType2OrientType.put("ntext", OType.STRING);
    dbmsType2OrientType.put("nchar", OType.STRING);
    dbmsType2OrientType.put("national char", OType.STRING);
    dbmsType2OrientType.put("national character", OType.STRING);
    dbmsType2OrientType.put("nvarchar", OType.STRING);
    dbmsType2OrientType.put("national char varying", OType.STRING);
    dbmsType2OrientType.put("national character varying", OType.STRING);


    /*
     * Numeric Types
     * (doc at https://msdn.microsoft.com/en-us/library/ms187745.aspx,
     * https://msdn.microsoft.com/en-us/library/ms187746.aspx, https://msdn.microsoft.com/en-us/library/ms173773.aspx )
     */
    dbmsType2OrientType.put("smallint", OType.SHORT);
    dbmsType2OrientType.put("int", OType.INTEGER);
    dbmsType2OrientType.put("bigint", OType.LONG);
    dbmsType2OrientType.put("tinyint", OType.SHORT);
    dbmsType2OrientType.put("decimal", OType.DECIMAL);
    dbmsType2OrientType.put("dec", OType.DECIMAL);
    dbmsType2OrientType.put("numeric", OType.DECIMAL);
    dbmsType2OrientType.put("real", OType.FLOAT);
    dbmsType2OrientType.put("float", OType.FLOAT);


    /*
     * Monetary Types
     * (doc at https://msdn.microsoft.com/en-us/library/ms179882.aspx )
     */
    dbmsType2OrientType.put("money", OType.DOUBLE);
    dbmsType2OrientType.put("smallmoney", OType.FLOAT);


    /*
     * Bit String Types
     * (doc at https://msdn.microsoft.com/en-us/library/ms177603.aspx )
     */
    dbmsType2OrientType.put("bit", OType.STRING);


    /*
     * Date/Time Types
     * (doc at https://msdn.microsoft.com/en-us/library/bb630352.aspx, https://msdn.microsoft.com/en-us/library/bb677243.aspx, https://msdn.microsoft.com/en-us/library/ms182418.aspx,
     *  https://msdn.microsoft.com/en-us/library/ms187819.aspx, https://msdn.microsoft.com/en-us/library/bb677335.aspx, https://msdn.microsoft.com/en-us/library/bb630289.aspx )
     */    
    dbmsType2OrientType.put("date", OType.DATE);
    dbmsType2OrientType.put("time", OType.STRING);
    dbmsType2OrientType.put("smalldatetime", OType.DATETIME);
    dbmsType2OrientType.put("datetime", OType.DATETIME);
    dbmsType2OrientType.put("datetime2", OType.DATETIME);
    dbmsType2OrientType.put("datetimeoffset", OType.DATETIME);


    /*
     * Binary Data Types
     * (doc at https://msdn.microsoft.com/en-us/library/ms188362.aspx, https://msdn.microsoft.com/en-us/library/ms187993.aspx )
     */
    dbmsType2OrientType.put("binary", OType.BINARY);
    dbmsType2OrientType.put("varbinary", OType.BINARY);
    dbmsType2OrientType.put("binary varying", OType.BINARY);
    dbmsType2OrientType.put("image", OType.BINARY);


    return dbmsType2OrientType;
  }

}
