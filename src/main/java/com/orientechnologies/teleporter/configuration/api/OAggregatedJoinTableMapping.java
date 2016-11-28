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

package com.orientechnologies.teleporter.configuration.api;

import java.util.List;

/**
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 *
 */

public class OAggregatedJoinTableMapping {

    private String tableName;           // mandatory
    private List<String> fromColumns;   // mandatory
    private List<String> toColumns;     // mandatory

    public OAggregatedJoinTableMapping(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getFromColumns() {
        return this.fromColumns;
    }

    public void setFromColumns(List<String> fromColumns) {
        this.fromColumns = fromColumns;
    }

    public List<String> getToColumns() {
        return this.toColumns;
    }

    public void setToColumns(List<String> toColumns) {
        this.toColumns = toColumns;
    }
}
