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

package com.orientechnologies.teleporter.importengine.rdbms.dbengine;

import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;
import java.util.List;

/**
 * Interface representing the query builder used by the DB Query Engine, hiding specific
 * implementation for each DBMS.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public interface OQueryBuilder {

  String countTableRecords(String currentTableName, String currentTableSchema);

  String getRecordById(OEntity entity, String[] propertyOfKey, String[] valueOfKey);

  String getRecordsByEntity(OEntity entity);

  String getRecordsFromMultipleEntities(List<OEntity> mappedEntities, String[][] columns);

  String getRecordsFromSingleTableByDiscriminatorValue(
      String discriminatorColumn, String currentDiscriminatorValue, OEntity entity);

  String getEntityTypeFromSingleTable(
      String discriminatorColumn, OEntity entity, String[] propertyOfKey, String[] valueOfKey);

  String buildAggregateTableFromHierarchicalBag(OHierarchicalBag bag);
}
