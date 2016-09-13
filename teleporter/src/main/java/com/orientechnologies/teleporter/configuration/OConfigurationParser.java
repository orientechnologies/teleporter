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

package com.orientechnologies.teleporter.configuration;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;

/**
 * It parses the configuration JSON and builds a OConfiguration object to handle all the information
 * in a easy way.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 *
 */

public class OConfigurationParser {

    public OConfiguration buildConfigurationFromJSON(ODocument jsonConfiguration) {

        OConfiguration configuration = new OConfiguration();

        // parsing vertices' configuration
        this.buildConfiguredVertices(configuration);

        // parsing edges' configuration
        this.buildConfiguredEdges(configuration);

        return configuration;
    }


    private void buildConfiguredVertices(OConfiguration configuration) {
    }

    private void buildConfiguredEdges(OConfiguration configuration) {
    }
}
