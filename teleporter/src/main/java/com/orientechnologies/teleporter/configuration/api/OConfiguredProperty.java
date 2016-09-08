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

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 *
 */

public class OConfiguredProperty {

    private String propertyName;
    private boolean isIncludedInMigration;
    private OType propertyType;
    private boolean isMandatory;
    private boolean isReadOnly;
    private boolean isNotNull;

    // may be null if the property is defined from scratch
    private OSourceTable sourceTable;
    private String originalColumnName;
    private String originalType;

}
