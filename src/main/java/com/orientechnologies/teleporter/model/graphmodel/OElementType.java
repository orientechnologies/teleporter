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

package com.orientechnologies.teleporter.model.graphmodel;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * It represents an Orient class. It could be a Vertex-Type or an Edge-Type in the graph model.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OElementType implements Comparable<OElementType> {

  protected String name;
  protected List<OModelProperty> properties;
  protected List<OModelProperty> inheritedProperties;
  protected Set<OModelProperty> allProperties;
  protected OElementType parentType;
  protected int inheritanceLevel;

  public OElementType(String type) {
    this.name = type;
    this.properties = new LinkedList<OModelProperty>();
    this.inheritedProperties = new LinkedList<OModelProperty>();
    this.allProperties = null;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String type) {
    this.name = type;
  }

  public List<OModelProperty> getProperties() {
    return this.properties;
  }

  public void setProperties(List<OModelProperty> properties) {
    this.properties = properties;
  }

  public List<OModelProperty> getInheritedProperties() {
    return this.inheritedProperties;
  }

  public void setInheritedProperties(List<OModelProperty> inheritedProperties) {
    this.inheritedProperties = inheritedProperties;
  }

  public OElementType getParentType() {
    return this.parentType;
  }

  public void setParentType(OElementType parentType) {
    this.parentType = parentType;
  }

  public int getInheritanceLevel() {
    return this.inheritanceLevel;
  }

  public void setInheritanceLevel(int inheritanceLevel) {
    this.inheritanceLevel = inheritanceLevel;
  }

  public OModelProperty getPropertyByOrdinalPosition(int position) {
    for (OModelProperty property : this.properties) {
      if (property.getOrdinalPosition() == position) {
        return property;
      }
    }
    return null;
  }

  public void removePropertyByName(String toRemove) {
    Iterator<OModelProperty> it = this.properties.iterator();
    OModelProperty currentProperty = null;

    while (it.hasNext()) {
      currentProperty = it.next();
      if (currentProperty.getName().equals(toRemove)) it.remove();
    }
  }

  public OModelProperty getPropertyByName(String name) {
    for (OModelProperty property : this.properties) {
      if (property.getName().equals(name)) {
        return property;
      }
    }
    return null;
  }

  public OModelProperty getInheritedPropertyByName(String name) {
    for (OModelProperty property : this.inheritedProperties) {
      if (property.getName().equals(name)) {
        return property;
      }
    }
    return null;
  }

  public OModelProperty getPropertyByNameAmongAll(String name) {
    for (OModelProperty property : this.getAllProperties()) {
      if (property.getName().equals(name)) {
        return property;
      }
    }
    return null;
  }

  // Returns properties and inherited properties
  public Set<OModelProperty> getAllProperties() {

    if (allProperties == null) {
      allProperties = new LinkedHashSet<OModelProperty>();
      allProperties.addAll(this.inheritedProperties);
      allProperties.addAll(this.properties);
    }

    return allProperties;
  }

  @Override
  public int compareTo(OElementType toCompare) {

    if (this.inheritanceLevel > toCompare.getInheritanceLevel()) return 1;
    else if (this.inheritanceLevel < toCompare.getInheritanceLevel()) return -1;
    else return this.name.compareTo(toCompare.getName());
  }
}
