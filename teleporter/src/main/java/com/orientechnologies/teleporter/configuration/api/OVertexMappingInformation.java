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
 * @email <gabriele.ponzi--at--gmail.com>
 *
 */

public class OVertexMappingInformation {

    private OConfiguredVertexClass belongingVertex;  // mandatory
    private List<OSourceTable> sourceTables;    // must be not empty!
    private String aggregationFunction;         // optional (not present when there is not any aggregation)

    public OVertexMappingInformation(OConfiguredVertexClass belongingVertex) {
        this.belongingVertex = belongingVertex;
    }

    public OConfiguredVertexClass getBelongingVertex() {
        return this.belongingVertex;
    }

    public void setBelongingVertex(OConfiguredVertexClass belongingVertex) {
        this.belongingVertex = belongingVertex;
    }

    public List<OSourceTable> getSourceTables() {
        return this.sourceTables;
    }

    public void setSourceTables(List<OSourceTable> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public String getAggregationFunction() {
        return this.aggregationFunction;
    }

    public void setAggregationFunction(String aggregationFunction) {
        this.aggregationFunction = aggregationFunction;
    }
}
