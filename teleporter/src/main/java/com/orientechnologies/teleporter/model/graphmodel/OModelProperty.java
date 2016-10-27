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
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public class OModelProperty {

  private String name;
  private int ordinalPosition;
  private String originalType;
  private String orientdbType;
  private boolean fromPrimaryKey;
  private OElementType belongingElementType;
  private boolean includedInMigration;

  // costraints
  private Boolean mandatory;
  private Boolean readOnly;
  private Boolean notNull;

  public OModelProperty(String name, int ordinalPosition, String originalType, boolean fromPrimaryKey, OElementType belongingElementType) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.originalType = originalType;
    this.orientdbType = null;
    this.fromPrimaryKey = fromPrimaryKey;
    this.belongingElementType = belongingElementType;
    this.includedInMigration = true;
  }

  public OModelProperty(String name, int ordinalPosition, String originalType, String orientdbType, boolean fromPrimaryKey, OElementType belongingElementType, boolean mandatory, boolean readOnly, boolean notNull) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.originalType = originalType;
    this.orientdbType = orientdbType;
    this.fromPrimaryKey = fromPrimaryKey;
    this.belongingElementType = belongingElementType;
    this.mandatory = mandatory;
    this.readOnly = readOnly;
    this.notNull = notNull;
    this.includedInMigration = true;
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

  public String getOriginalType() {
    return this.originalType;
  }

  public void setOriginalType(String attributeType) {
    this.originalType = attributeType;
  }

  public String getOrientdbType() {
    return orientdbType;
  }

  public void setOrientdbType(String orientdbType) {
    this.orientdbType = orientdbType;
  }

  public boolean isFromPrimaryKey() {
    return this.fromPrimaryKey;
  }

  public void setFromPrimaryKey(boolean fromPrimaryKey) {
    this.fromPrimaryKey = fromPrimaryKey;
  }

  public OElementType getBelongingElementType() {
    return belongingElementType;
  }

  public void setBelongingElementType(OElementType belongingElementType) {
    this.belongingElementType = belongingElementType;
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

  public boolean isIncludedInMigration() {
    return includedInMigration;
  }

  public void setIncludedInMigration(boolean includedInMigration) {
    this.includedInMigration = includedInMigration;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((originalType == null) ? 0 : originalType.hashCode());
    result = prime * result + (fromPrimaryKey ? 1231 : 1237);
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ordinalPosition;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    OModelProperty that = (OModelProperty) obj;
    if(this.name.equals(that.getName()) && this.ordinalPosition == that.getOrdinalPosition() && 
        this.originalType.equals(that.getOriginalType()) && this.isFromPrimaryKey() == that.isFromPrimaryKey())
      return true;
    return false;
  }

  public String toString() {
    String s = "";
    s += this.ordinalPosition + ": " + this.name + " ( " + this.orientdbType + " )";
    return s;
  }

}
