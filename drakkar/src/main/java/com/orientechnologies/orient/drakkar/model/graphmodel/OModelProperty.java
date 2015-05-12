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

/**
 * Class which holds all the attributes of a vertex property obtained from
 * the transformation of an attribute belonging to an entity of the source DB schema.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OModelProperty {

  private String name;
  private int ordinalPosition;
  private String propertyType;
  private boolean fromPrimaryKey;
  // other constraints

  public OModelProperty(String name, int ordinalPosition, String propertyType, boolean fromPrimaryKey) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.propertyType = propertyType;
    this.fromPrimaryKey = fromPrimaryKey;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  public void setOrdinalPosition(int ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
  }

  public String getPropertyType() {
    return this.propertyType;
  }

  public void setPropertyType(String attributeType) {
    this.propertyType = attributeType;
  }

  public boolean isFromPrimaryKey() {
    return this.fromPrimaryKey;
  }

  public void setFromPrimaryKey(boolean fromPrimaryKey) {
    this.fromPrimaryKey = fromPrimaryKey;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((propertyType == null) ? 0 : propertyType.hashCode());
    result = prime * result + (fromPrimaryKey ? 1231 : 1237);
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ordinalPosition;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    OModelProperty that = (OModelProperty) obj;
    if(this.name.equals(that.getName()) && this.ordinalPosition == that.getOrdinalPosition() && 
        this.propertyType.equals(that.getPropertyType()) && this.isFromPrimaryKey() == that.isFromPrimaryKey())
      return true;
    return false;
  }

  public String toString() {
    String s = "[" + this.propertyType + "]";
    return s;
  }

}
