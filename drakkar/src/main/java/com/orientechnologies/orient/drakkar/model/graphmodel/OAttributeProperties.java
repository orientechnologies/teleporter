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
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 *
 */

public class OAttributeProperties {
  
  private int ordinalPosition;
  private String attributeType;
  private boolean fromPrimaryKey;
  // other constraints
  
  public OAttributeProperties(int ordinalPosition, String attributeType, boolean fromPrimaryKey) {
    this.ordinalPosition = ordinalPosition;
    this.attributeType = attributeType;
    this.fromPrimaryKey = fromPrimaryKey;
  }

  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  public void setOrdinalPosition(int ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
  }

  public String getAttributeType() {
    return this.attributeType;
  }

  public void setAttributeType(String attributeType) {
    this.attributeType = attributeType;
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
    result = prime * result + ((attributeType == null) ? 0 : attributeType.hashCode());
    result = prime * result + (fromPrimaryKey ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
   OAttributeProperties that = (OAttributeProperties) obj;
   if(this.attributeType.equals(that.getAttributeType()) && this.isFromPrimaryKey() == that.isFromPrimaryKey())
     return true;
   return false;
  }
  
  public String toString() {
    String s = "[" + this.attributeType + "]";
    return s;
  }
  
  
  

}
