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

package com.orientechnologies.teleporter.mapper.rdbms;

import com.orientechnologies.teleporter.model.dbschema.OAttribute;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is responsible to map 2 classes of objects: OEntity and OVertexType.
 * the following values are mapped:
 * - name of the entity -> name of the vertex type
 * - each attribute of the entity -> correspondent property of the vertex type
 * - each property of the vertex type -> correspondent attribute of the entity
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */

public class OClassMapper {

  public OEntity             entity;
  public OVertexType         vertexType;
  public Map<String, String> attribute2property;
  public Map<String, String> property2attribute;

  public OClassMapper(OEntity entity, OVertexType vertexType, Map<String, String> attribute2property,
      Map<String, String> property2attribute) {
    this.entity = entity;
    this.vertexType = vertexType;
    this.attribute2property = attribute2property;
    this.property2attribute = property2attribute;
  }

  public OEntity getEntity() {
    return entity;
  }

  public void setEntity(OEntity entity) {
    this.entity = entity;
  }

  public OVertexType getVertexType() {
    return vertexType;
  }

  public void setVertexType(OVertexType vertexType) {
    this.vertexType = vertexType;
  }

  public String getAttributeByProperty(String property) {
    return this.property2attribute.get(property);
  }

  public String getPropertyByAttribute(String attribute) {
    return this.attribute2property.get(attribute);
  }

  @Override
  public int hashCode() {
    int result = entity.hashCode();
    result = 31 * result + vertexType.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OClassMapper that = (OClassMapper) o;

    if (!entity.equals(that.entity))
      return false;
    if (!vertexType.equals(that.vertexType))
      return false;
    if (!attribute2property.equals(that.attribute2property))
      return false;
    return property2attribute.equals(that.property2attribute);
  }

  @Override
  public String toString() {
    String s = "{" + "Entity = " + entity.getName() + ", Vertex-Type = " + vertexType.getName() + ", attributes2properties: ";

    s += "[";
    for (String attribute : this.attribute2property.keySet()) {
      s += attribute + " --> " + attribute2property.get(attribute) + ", ";
    }
    s = s.substring(0, s.length() - 1);
    s += "]}";
    return s;
  }
}
