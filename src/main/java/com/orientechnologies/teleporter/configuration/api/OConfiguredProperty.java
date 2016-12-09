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

public class OConfiguredProperty {

    private String propertyName;                // mandatory
    private boolean isIncludedInMigration;      // mandatory
    private String propertyType;                // mandatory
    private int ordinalPosition;                // mandatory
    private boolean isMandatory;                // mandatory
    private boolean isReadOnly;                 // mandatory
    private boolean isNotNull;                  // mandatory
    private OConfiguredPropertyMapping propertyMapping;   // may be null if the property is defined from scratch (only schema definition)

    public OConfiguredProperty(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return this.propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public boolean isIncludedInMigration() {
        return this.isIncludedInMigration;
    }

    public void setIncludedInMigration(boolean includedInMigration) {
        this.isIncludedInMigration = includedInMigration;
    }

    public String getPropertyType() {
        return this.propertyType;
    }

    public void setPropertyType(String propertyType) {
        this.propertyType = propertyType;
    }

    public int getOrdinalPosition() {
        return this.ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public boolean isMandatory() {
        return this.isMandatory;
    }

    public void setMandatory(boolean mandatory) {
        this.isMandatory = mandatory;
    }

    public boolean isReadOnly() {
        return this.isReadOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.isReadOnly = readOnly;
    }

    public boolean isNotNull() {
        return this.isNotNull;
    }

    public void setNotNull(boolean notNull) {
        this.isNotNull = notNull;
    }

    public OConfiguredPropertyMapping getPropertyMapping() {
        return this.propertyMapping;
    }

    public void setPropertyMapping(OConfiguredPropertyMapping propertyMapping) {
        this.propertyMapping = propertyMapping;
    }
}
