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
import com.orientechnologies.teleporter.model.dbschema.OEntity;

import java.util.List;

/**
 * Query Builder for MySQL DBMS. It extends the OCommonQueryBuilder class and overrides only the needed methods.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OMysqlQueryBuilder extends OCommonQueryBuilder {

  public OMysqlQueryBuilder() {
    this.quote = "`";
  }

  /**
   * MySQL does not allow full outer join, so this query is expressed as UNION of LEFT and RIGHT JOIN.
   *
   * @param mappedEntities
   * @param columns
   *
   * @return
   */

  @Override
  public String getRecordsFromMultipleEntities(List<OEntity> mappedEntities, String[][] columns) {
    String query;

    OEntity first = mappedEntities.get(0);
    if (first.getSchemaName() != null)
      query = "select * from " + first.getSchemaName() + "." + this.quote + first.getName() + this.quote + " as t0\n";
    else
      query = "select * from " + this.quote + first.getName() + this.quote + " as t0\n";

    for (int i = 1; i < mappedEntities.size(); i++) {
      OEntity currentEntity = mappedEntities.get(i);
      query +=
          " left join " + currentEntity.getSchemaName() + "." + this.quote + currentEntity.getName() + this.quote + " as t" + i;
      query += " on t" + (i - 1) + "." + this.quote + columns[i - 1][0] + this.quote + " = t" + i + "." + this.quote + columns[i][0]
          + this.quote;

      for (int k = 1; k < columns[i].length; k++) {
        query +=
            " and t" + (i - 1) + "." + this.quote + columns[i - 1][k] + this.quote + " = t" + i + "." + this.quote + columns[i][k]
                + this.quote;
      }

      query += "\n";
    }

    query += "UNION\n";

    if (first.getSchemaName() != null)
      query += "select * from " + first.getSchemaName() + "." + this.quote + first.getName() + this.quote + " as t0\n";
    else
      query += "select * from " + this.quote + first.getName() + this.quote + " as t0\n";

    for (int i = 1; i < mappedEntities.size(); i++) {
      OEntity currentEntity = mappedEntities.get(i);
      query +=
          " right join " + currentEntity.getSchemaName() + "." + this.quote + currentEntity.getName() + this.quote + " as t" + i;
      query += " on t" + (i - 1) + "." + this.quote + columns[i - 1][0] + this.quote + " = t" + i + "." + this.quote + columns[i][0]
          + this.quote;

      for (int k = 1; k < columns[i].length; k++) {
        query +=
            " and t" + (i - 1) + "." + this.quote + columns[i - 1][k] + this.quote + " = t" + i + "." + this.quote + columns[i][k]
                + this.quote;
      }

      query += "\n";
    }

    return query;
  }

}
