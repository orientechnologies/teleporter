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

/**
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 *
 */

public class OSplittingEdgeInformation {

    private String fromVertexClass;
    private String toVertexClass;

    public OSplittingEdgeInformation(String fromVertexClass, String toVertexClass) {
        this.fromVertexClass = fromVertexClass;
        this.toVertexClass = toVertexClass;
    }

    public String getFromVertexClass() {
        return this.fromVertexClass;
    }

    public void setFromVertexClass(String fromVertexClass) {
        this.fromVertexClass = fromVertexClass;
    }

    public String getToVertexClass() {
        return this.toVertexClass;
    }

    public void setToVertexClass(String toVertexClass) {
        this.toVertexClass = toVertexClass;
    }
}
