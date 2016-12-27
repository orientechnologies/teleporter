/*
 * Copyright 2016 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.teleporter.importengine.rdbms.dbengine;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.model.dbschema.OAttribute;
import com.orientechnologies.teleporter.model.dbschema.OEntity;

import java.util.List;

/**
 * Query Builder for PostgreSQL DBMS. It extends the OCommonQueryBuilder class and overrides only the needed methods.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OPostgreSQLQueryBuilder extends OCommonQueryBuilder {

  public String buildGeospatialQuery(OEntity entity, List<String> geospatialTypes, OTeleporterContext context) {

    String query = "select ";

    for (OAttribute currentAttribute : entity.getAllAttributes()) {
      if (this.isGeospatial(geospatialTypes, currentAttribute.getDataType()))
        query += "ST_AsText(" + quote + currentAttribute.getName() + quote + ") as " + currentAttribute.getName() + ",";
      else
        query += quote + currentAttribute.getName() + quote + ",";
    }

    query = query.substring(0, query.length() - 1);

    String entitySchema = entity.getSchemaName();

    if (entitySchema != null)
      query += " from " + entitySchema + "." + quote + entity.getName() + quote;
    else
      query += " from " + quote + entity.getName() + quote;

    return query;
  }

  public boolean isGeospatial(List<String> geospatialTypes, String type) {
    return geospatialTypes.contains(type);
  }
}
