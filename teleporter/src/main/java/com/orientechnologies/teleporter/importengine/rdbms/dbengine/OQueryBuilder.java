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
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;
import com.orientechnologies.teleporter.persistence.util.OQueryResult;

import java.util.List;

/**
 * Interface representing the query builder used by the DB Query Engine, hiding specific implementation for each DBMS.
 *
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public interface OQueryBuilder {

    String countTableRecords(String currentTableName, String currentTableSchema);
    String getRecordById(OEntity entity, String[] propertyOfKey, String[] valueOfKey);
    String getRecordsByEntity(OEntity entity);
    String getRecordsFromMultipleEntities(List<OEntity> mappedEntities, String[][] columns);
    String getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, OEntity entity);
    String getEntityTypeFromSingleTable(String discriminatorColumn, OEntity entity, String[] propertyOfKey, String[] valueOfKey);
    String buildAggregateTableFromHierarchicalBag(OHierarchicalBag bag);
}
