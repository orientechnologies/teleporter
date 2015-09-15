/*
 * Copyright 2015 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.teleporter.model.graphmodel;

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
    String s = "";
    s += this.ordinalPosition + ": " + this.name + " ( " + this.propertyType + " )";
    return s;
  }

}
