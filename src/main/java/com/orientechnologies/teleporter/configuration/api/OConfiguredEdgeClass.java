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

public class OConfiguredEdgeClass extends OConfiguredClass {

    // mappings and splittingEdgeInfo are mutually exclusive
    private List<OEdgeMappingInformation> mappings;                  // mandatory
    private OSplittingEdgeInformation splittingEdgeInfo;             // mandatory
    private boolean isLogical;                                       // optional

    public OConfiguredEdgeClass(String edgeName, OConfiguration globalConfiguration) {
        super(edgeName, globalConfiguration);
        this.isLogical = false;
    }

    public List<OEdgeMappingInformation> getMappings() {
        return this.mappings;
    }

    public void setMappings(List<OEdgeMappingInformation> mappings) {
        this.mappings = mappings;
    }

    public OSplittingEdgeInformation getSplittingEdgeInfo() {
        return this.splittingEdgeInfo;
    }

    public void setSplittingEdgeInfo(OSplittingEdgeInformation splittingEdgeInfo) {
        this.splittingEdgeInfo = splittingEdgeInfo;
    }

    public boolean isLogical() {
        return this.isLogical;
    }

    public void setLogical(boolean logical) {
        this.isLogical = logical;
    }
}
