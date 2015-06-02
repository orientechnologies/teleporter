/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.drakkar.model.graphmodel;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * It represents an Orient class. It could be a Vertex-Type or an Edge-Type in the graph model.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OElementType implements Comparable<OElementType> {
  
  protected String name;
  protected List<OModelProperty> properties;
  protected OElementType parentType;
  protected int inheritanceLevel;

  public OElementType(String type) {
    this.name = type;
    this.properties = new LinkedList<OModelProperty>();
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

  public void removePropertyByName(String toRemove) {
    Iterator<OModelProperty> it = this.properties.iterator();
    OModelProperty currentProperty = null;

    while (it.hasNext()) {
      currentProperty = it.next();
      if(currentProperty.getName().equals(toRemove))
        it.remove();
    }
  }
  
  public OModelProperty getPropertyByName(String name) {
    for(OModelProperty property: this.properties) {
      if(property.getName().equals(name)) {
        return property;
      }
    }
    return null;
  }

  @Override
  public int compareTo(OElementType toCompare) {
    
    if(this.inheritanceLevel > toCompare.getInheritanceLevel())
      return 1;
    else if(this.inheritanceLevel < toCompare.getInheritanceLevel())
      return -1;
    else
      return this.name.compareTo(toCompare.getName());

  }
}
