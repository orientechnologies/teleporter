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

package com.orientechnologies.teleporter.model.dbschema;

/**
 * It represents an attribute of an entity.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */

public class OAttribute implements Comparable<OAttribute> {

  private String  name;
  private int     ordinalPosition;
  private String  dataType;
  private OEntity belongingEntity;

  public OAttribute(String name, int ordinalPosition, String dataType, OEntity belongingEntity) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.dataType = dataType;
    this.belongingEntity = belongingEntity;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getOrdinalPosition() {
    return this.ordinalPosition;
  }

  public void setOrdinalPosition(int ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
  }

  public String getDataType() {
    return this.dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public OEntity getBelongingEntity() {
    return this.belongingEntity;
  }

  public void setBelongingEntity(OEntity belongingEntity) {
    this.belongingEntity = belongingEntity;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  public boolean equals(Object o) {
    OAttribute that = (OAttribute) o;
    if (this.name.equals(that.getName()) && this.dataType.equals(that.getDataType())) {
      return true;
    } else
      return false;

  }

  @Override
  public int compareTo(OAttribute attributeToCompare) {

    if (this.ordinalPosition > attributeToCompare.getOrdinalPosition())
      return 0;

    else if (this.ordinalPosition < attributeToCompare.getOrdinalPosition())
      return -1;

    else
      return 1;
  }

  public String toString() {
    String s = "";
    s += this.ordinalPosition + ": " + this.name + " ( " + this.dataType + " )";
    return s;
  }

}
