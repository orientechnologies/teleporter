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

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 *
 */

public class OConfiguredVertexClass extends OConfiguredClass {

    private List<String> externalKeyProps;
    private OVertexMappingInformation mapping;                  // mandatory

    // boolean value used to specify if the configured vertex was already analyzed and applied to the graph model
    private boolean alreadyAnalyzed;

    public OConfiguredVertexClass(String vertexName, OConfiguration globalConfiguration) {
        super(vertexName, globalConfiguration);
        this.externalKeyProps = new LinkedList<String>();
        this.alreadyAnalyzed = false;
    }

    public OVertexMappingInformation getMapping() {
        return this.mapping;
    }

    public void setMapping(OVertexMappingInformation mapping) {
        this.mapping = mapping;
    }

    public List<String> getExternalKeyProps() {
        return externalKeyProps;
    }

    public void setExternalKeyProps(List<String> externalKeyProps) {
        this.externalKeyProps = externalKeyProps;
    }

    public boolean isAlreadyAnalyzed() {
        return alreadyAnalyzed;
    }

    public void setAlreadyAnalyzed(boolean alreadyAnalyzed) {
        this.alreadyAnalyzed = alreadyAnalyzed;
    }
}
