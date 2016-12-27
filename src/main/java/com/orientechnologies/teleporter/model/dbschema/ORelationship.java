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
 * It represents the relationship between two entities (foreign and parent entity)
 * based on the importing of a single primary key (composite or not) through a foreign key.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */

public class ORelationship {

  private OEntity     foreignEntity;        // Entity importing the key (starting entity)
  private OEntity     parentEntity;        // Entity exporting the key (arrival entity)
  private OForeignKey foreignKey;
  private OPrimaryKey primaryKey;
  private String      direction;               // represents the direction of the relationship

  public ORelationship(OEntity foreignEntity, OEntity parentEntity) {
    this.foreignEntity = foreignEntity;
    this.parentEntity = parentEntity;
    this.direction = "direct";
  }

  public ORelationship(OEntity foreignEntity, OEntity parentEntity, OForeignKey foreignKey, OPrimaryKey primaryKey) {
    this.foreignEntity = foreignEntity;
    this.parentEntity = parentEntity;
    this.foreignKey = foreignKey;
    this.primaryKey = primaryKey;
    this.direction = "direct";
  }

  public OEntity getForeignEntity() {
    return this.foreignEntity;
  }

  public void setForeignEntity(OEntity foreignEntity) {
    this.foreignEntity = foreignEntity;
  }

  public OEntity getParentEntity() {
    return this.parentEntity;
  }

  public void setParentEntity(OEntity parentEntity) {
    this.parentEntity = parentEntity;
  }

  public OForeignKey getForeignKey() {
    return foreignKey;
  }

  public void setForeignKey(OForeignKey foreignKey) {
    this.foreignKey = foreignKey;
  }

  public OPrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(OPrimaryKey primaryKey) {
    this.primaryKey = primaryKey;
  }

  public String getDirection() {
    return this.direction;
  }

  public void setDirection(String direction) {
    this.direction = direction;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((foreignEntity == null) ? 0 : foreignEntity.hashCode());
    result = prime * result + ((parentEntity == null) ? 0 : parentEntity.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    ORelationship that = (ORelationship) obj;
    if (this.foreignEntity.equals(that.getForeignEntity()) && this.parentEntity.equals(that.getParentEntity())) {
      if (this.foreignKey.equals(that.getForeignKey()) && this.primaryKey.equals(that.getPrimaryKey())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "ORelationship [foreignEntity=" + foreignEntity.getName() + ", parentEntity=" + parentEntity.getName() + ", Foreign key="
        + this.foreignKey.toString() + ", Primary key=" + this.primaryKey.toString() + "]";
  }

}
