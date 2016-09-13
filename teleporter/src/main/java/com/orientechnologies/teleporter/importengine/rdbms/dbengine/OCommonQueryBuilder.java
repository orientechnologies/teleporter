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
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;

import java.util.Iterator;
import java.util.List;

/**
 * Class implementing OQueryBuilder interface: the implemented methods are usable with every DBMS.
 * This class is extended by DBMS-specific query builder implementations.
 *
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OCommonQueryBuilder implements OQueryBuilder {

    protected String quote;

    public OCommonQueryBuilder() {
        this.quote = "\"";
    }


    @Override
    public String countTableRecords(String currentTableName, String currentTableSchema, OTeleporterContext context) {
        String query;

        if(currentTableSchema != null)
            query = "select count(*) from " + currentTableSchema + "." + this.quote + currentTableName + this.quote;
        else
            query = "select count(*) from " + quote + currentTableName + this.quote;

        return query;
    }

    @Override
    public String getRecordById(OEntity entity, String[] propertyOfKey, String[] valueOfKey, OTeleporterContext context) {
        String query;

        String entityName = entity.getName();
        String entitySchema = entity.getSchemaName();

        if(entitySchema != null)
            query = "select * from " + entitySchema + "." + this.quote + entityName + this.quote + " where ";
        else
            query = "select * from " + this.quote + entityName + this.quote + " where ";

        query += this.quote + propertyOfKey[0] + this.quote + " = '" + valueOfKey[0] + "'";

        if(propertyOfKey.length > 1) {
            for(int i=1; i<propertyOfKey.length; i++) {
                query += " and " + this.quote + propertyOfKey[i] + this.quote + " = '" + valueOfKey[i] + "'";
            }
        }

        return query;
    }

    @Override
    public String getRecordsByEntity(OEntity entity, OTeleporterContext context) {
        String query;

        String entityName = entity.getName();
        String entitySchema = entity.getSchemaName();

        if(entitySchema != null)
            query = "select * from " + entitySchema + "." + this.quote + entityName + this.quote;
        else
            query = "select * from " + this.quote + entityName + this.quote;

        return query;
    }

    @Override
    public String getRecordsFromMultipleEntities(List<OEntity> mappedEntities, String[][] columns, OTeleporterContext context) {
        String query;

        OEntity first = mappedEntities.get(0);
        if(first.getSchemaName() != null)
            query = "select * from " + first.getSchemaName() + "." + this.quote + first.getName() + this.quote + " as t0\n";
        else
            query = "select * from " + this.quote + first.getName() + this.quote + " as t0\n";

        for(int i = 1; i<mappedEntities.size(); i++) {
            OEntity currentEntity = mappedEntities.get(i);
            query += " full outer join " + currentEntity.getSchemaName() + "." + this.quote + currentEntity.getName() + this.quote + " as t" + i;
            query += " on t" + (i-1) + "." + this.quote + columns[i-1][0] + this.quote + " = t" + i + "." + this.quote + columns[i][0] + this.quote;

            for(int k=1; k<columns[i].length; k++) {
                query += " and t" + (i-1) + "." + this.quote + columns[i-1][k] + this.quote + " = t" + i + "." + this.quote + columns[i][k] + this.quote;
            }

            query += "\n";
        }

        return query;
    }

    @Override
    public String getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, OEntity entity, OTeleporterContext context) {
        String query;

        String entityName = entity.getName();
        String entitySchema = entity.getSchemaName();

        if(entitySchema != null)
            query = "select * from " + entitySchema + "." + this.quote + entityName + this.quote;
        else
            query = "select * from " + this.quote + entityName + this.quote;

        query += " where " + this.quote + discriminatorColumn + this.quote + "='" + currentDiscriminatorValue + "'";

        return query;
    }

    @Override
    public String getEntityTypeFromSingleTable(String discriminatorColumn, OEntity physicalEntity, String[] propertyOfKey, String[] valueOfKey, OTeleporterContext context) {
        String query;

        String physicalEntityName = physicalEntity.getName();
        String entitySchema = physicalEntity.getSchemaName();

        if(entitySchema != null)
            query = "select " + discriminatorColumn + " from " + entitySchema + "." + this.quote + physicalEntityName + this.quote + " where ";
        else
            query = "select " + discriminatorColumn + " from " + this.quote + physicalEntityName + this.quote + " where ";

        query += this.quote + propertyOfKey[0] + this.quote + " = '" + valueOfKey[0] + "'";

        if(propertyOfKey.length > 1) {
            for(int i=1; i<propertyOfKey.length; i++) {
                query += " and " + this.quote + propertyOfKey[i] + this.quote + " = '" + valueOfKey[i] + "'";
            }
        }

        return query;
    }

    @Override
    public String buildAggregateTableFromHierarchicalBag(OHierarchicalBag bag, OTeleporterContext context) {
        String query;

        Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
        OEntity rootEntity = it.next();

        if(rootEntity.getSchemaName() != null)
            query = "select * from " + rootEntity.getSchemaName() + "." + this.quote + rootEntity.getName() + this.quote + " as t0\n";
        else
            query = "select * from " + this.quote + rootEntity.getName() + this.quote + " as t0\n";

        String[] rootEntityPropertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];  // collects the attributes of the root-entity's primary key

        // filling the rootPropertyOfKey from the primary key of the rootEntity
        for(int j=0; j<rootEntity.getPrimaryKey().getInvolvedAttributes().size(); j++) {
            rootEntityPropertyOfKey[j] = rootEntity.getPrimaryKey().getInvolvedAttributes().get(j).getName();
        }

        String[] currentEntityPropertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];  // collects the attributes of the current-entity's primary key

        OEntity currentEntity;
        int thTable = 1;
        for(int i=1; i<bag.getDepth2entities().size(); i++) {
            it = bag.getDepth2entities().get(i).iterator();

            while(it.hasNext()) {

                currentEntity = it.next();
                int index = 0;
                for(OAttribute attribute: currentEntity.getPrimaryKey().getInvolvedAttributes()) {
                    currentEntityPropertyOfKey[index] = attribute.getName();
                    index++;
                }

                if(currentEntity.getSchemaName() != null)
                    query += "left join " + currentEntity.getSchemaName() + "." + this.quote + currentEntity.getName() + this.quote;
                else
                    query += "left join " + this.quote + currentEntity.getName() + this.quote;

                query += " as t" + thTable +
                        " on t0." + this.quote + rootEntityPropertyOfKey[0] + this.quote + " = t" + thTable + "." + this.quote + currentEntityPropertyOfKey[0] + this.quote;

                for(int k=1; k<currentEntityPropertyOfKey.length; k++) {
                    query += " and " + this.quote + rootEntityPropertyOfKey[k] + this.quote + " = t" + thTable + "." + this.quote + currentEntityPropertyOfKey[0] + this.quote;
                }

                query += "\n";
                thTable++;
            }
        }


        return query;
    }
}
