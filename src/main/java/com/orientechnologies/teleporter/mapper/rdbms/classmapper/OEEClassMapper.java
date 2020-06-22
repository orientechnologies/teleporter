/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.teleporter.mapper.rdbms.classmapper;

import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import java.util.Map;

/**
 * This class is responsible to map 2 classes of objects: OEntity and OEdgeType. the following
 * values are mapped: - name of the entity -> name of the edge type - each attribute of the entity
 * -> correspondent property of the edge type - each property of the edge type -> correspondent
 * attribute of the entity
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OEEClassMapper extends OEntityClassMapper {

  private OEdgeType edgeType;

  public OEEClassMapper(
      OEntity entity,
      OEdgeType edgeType,
      Map<String, String> attribute2property,
      Map<String, String> property2attribute) {
    super(entity, attribute2property, property2attribute);
    this.edgeType = edgeType;
  }

  public OEdgeType getEdgeType() {
    return edgeType;
  }

  public void setEdgeType(OEdgeType edgeType) {
    this.edgeType = edgeType;
  }

  @Override
  public int hashCode() {
    int result = entity.hashCode();
    result = 31 * result + edgeType.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OEEClassMapper that = (OEEClassMapper) o;

    if (!entity.equals(that.entity)) return false;
    if (!edgeType.equals(that.edgeType)) return false;
    if (!attribute2property.equals(that.attribute2property)) return false;
    return property2attribute.equals(that.property2attribute);
  }

  @Override
  public String toString() {
    String s =
        "{"
            + "Entity = "
            + entity.getName()
            + ", Edge-Type = "
            + edgeType.getName()
            + ", attributes2properties: ";

    s += "[";
    for (String attribute : this.attribute2property.keySet()) {
      s += attribute + " --> " + attribute2property.get(attribute) + ", ";
    }
    s = s.substring(0, s.length() - 1);
    s += "]}";
    return s;
  }
}
