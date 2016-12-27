/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.teleporter.model.graphmodel;

/**
 * Class which holds all the attributes of a vertex property obtained from
 * the transformation of an attribute belonging to an entity of the source DB schema.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */

public class OModelProperty {

  private String  name;
  private int     ordinalPosition;
  private String  propertyType;
  private boolean fromPrimaryKey;
  // costraints
  private Boolean mandatory;
  private Boolean readOnly;
  private Boolean notNull;

  public OModelProperty(String name, int ordinalPosition, String propertyType, boolean fromPrimaryKey) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.propertyType = propertyType;
    this.fromPrimaryKey = fromPrimaryKey;
  }

  public OModelProperty(String name, int ordinalPosition, String propertyType, boolean fromPrimaryKey, boolean mandatory,
      boolean readOnly, boolean notNull) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.propertyType = propertyType;
    this.fromPrimaryKey = fromPrimaryKey;
    this.mandatory = mandatory;
    this.readOnly = readOnly;
    this.notNull = notNull;
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

  public Boolean isMandatory() {
    return this.mandatory;
  }

  public void setMandatory(Boolean mandatory) {
    this.mandatory = mandatory;
  }

  public Boolean isReadOnly() {
    return this.readOnly;
  }

  public void setReadOnly(Boolean readOnly) {
    this.readOnly = readOnly;
  }

  public Boolean isNotNull() {
    return this.notNull;
  }

  public void setNotNull(Boolean notNull) {
    this.notNull = notNull;
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
    if (this.name.equals(that.getName()) && this.ordinalPosition == that.getOrdinalPosition() && this.propertyType
        .equals(that.getPropertyType()) && this.isFromPrimaryKey() == that.isFromPrimaryKey())
      return true;
    return false;
  }

  public String toString() {
    String s = "";
    s += this.ordinalPosition + ": " + this.name + " ( " + this.propertyType + " )";
    return s;
  }

}
