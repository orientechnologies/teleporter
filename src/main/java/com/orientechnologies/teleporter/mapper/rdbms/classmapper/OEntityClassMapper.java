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

package com.orientechnologies.teleporter.mapper.rdbms.classmapper;

import com.orientechnologies.teleporter.model.dbschema.OEntity;

import java.util.Map;

/**
 * This abstract class is extended by all the classes responsible to map 2 classes of objects where one of them is an Entity.
 * Subclasses:
 *  - OEVClassMapper: OEntity --> OVertexType
 *  - OEEClassMapper: OEntity --> OEdgeType
 *
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public abstract class OEntityClassMapper {

    protected OEntity entity;
    protected Map<String,String> attribute2property;
    protected Map<String,String> property2attribute;

    public OEntityClassMapper(OEntity entity, Map<String,String> attribute2property, Map<String,String> property2attribute) {
        this.entity = entity;
        this.attribute2property = attribute2property;
        this.property2attribute = property2attribute;
    }

    public OEntity getEntity() {
        return entity;
    }

    public void setEntity(OEntity entity) {
        this.entity = entity;
    }

    public Map<String, String> getAttribute2property() {
        return attribute2property;
    }

    public void setAttribute2property(Map<String, String> attribute2property) {
        this.attribute2property = attribute2property;
    }

    public Map<String, String> getProperty2attribute() {
        return property2attribute;
    }

    public void setProperty2attribute(Map<String, String> property2attribute) {
        this.property2attribute = property2attribute;
    }

    public String getAttributeByProperty(String property) {
        return this.property2attribute.get(property);
    }

    public String getPropertyByAttribute(String attribute) {
        return this.attribute2property.get(attribute);
    }

    public boolean containsAttribute(String attributeName) {
        return this.attribute2property.containsKey(attributeName);
    }

    public boolean containsProperty(String propertyName) {
        return this.property2attribute.containsKey(propertyName);
    }


}
