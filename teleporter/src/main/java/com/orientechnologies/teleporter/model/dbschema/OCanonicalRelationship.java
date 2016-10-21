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

import java.util.List;

/**
 * It represents a canonical relationship between two entities (foreign and parent entity)
 * based on the importing of a single primary key (composite or not) through a foreign key.
 * 
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 * 
 */

public class OCanonicalRelationship extends ORelationship {

  private OForeignKey foreignKey;
  private OPrimaryKey primaryKey;

  public OCanonicalRelationship(OEntity foreignEntity, OEntity parentEntity) {
    this.foreignEntity = foreignEntity;
    this.parentEntity = parentEntity;
    this.direction = "direct";
  }

  public OCanonicalRelationship(OEntity foreignEntity, OEntity parentEntity, OForeignKey foreignKey, OPrimaryKey primaryKey) {
    this.foreignEntity = foreignEntity;
    this.parentEntity = parentEntity;
    this.foreignKey = foreignKey;
    this.primaryKey = primaryKey;
    this.direction = "direct";
  }

  @Override
  public List<OAttribute> getFromColumns() {
    return this.foreignKey.getInvolvedAttributes();
  }

  @Override
  public List<OAttribute> getToColumns() {
    return this.primaryKey.getInvolvedAttributes();
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


  @Override
  public boolean equals(Object obj) {
    OCanonicalRelationship that = (OCanonicalRelationship) obj;
    if(this.foreignEntity.equals(that.getForeignEntity()) && this.parentEntity.equals(that.getParentEntity())) {
      if(this.foreignKey.equals(that.getForeignKey()) && this.primaryKey.equals(that.getPrimaryKey())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "OCanonicalRelationship [foreignEntity=" + foreignEntity.getName() + ", parentEntity=" + parentEntity.getName()
        + ", Foreign key=" + this.foreignKey.toString() + ", Primary key=" + this.primaryKey.toString() + "]";
  }

}
