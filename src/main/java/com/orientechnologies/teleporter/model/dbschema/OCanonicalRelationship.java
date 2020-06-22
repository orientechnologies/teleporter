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

package com.orientechnologies.teleporter.model.dbschema;

import java.util.List;

/**
 * It represents a canonical relationship between two entities (foreign and parent entity) based on
 * the importing of a single primary key (composite or not) through a foreign key.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OCanonicalRelationship extends ORelationship {

  private OForeignKey foreignKey;
  private OPrimaryKey primaryKey;

  public OCanonicalRelationship(OEntity foreignEntity, OEntity parentEntity) {
    this.foreignEntity = foreignEntity;
    this.parentEntity = parentEntity;
    this.direction = "direct";
  }

  public OCanonicalRelationship(
      OEntity foreignEntity, OEntity parentEntity, OForeignKey foreignKey, OPrimaryKey primaryKey) {
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
    if (this.foreignEntity.equals(that.getForeignEntity())
        && this.parentEntity.equals(that.getParentEntity())) {
      if (this.foreignKey.equals(that.getForeignKey())
          && this.primaryKey.equals(that.getPrimaryKey())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "OCanonicalRelationship [foreignEntity="
        + foreignEntity.getName()
        + ", parentEntity="
        + parentEntity.getName()
        + ", Foreign key="
        + this.foreignKey.toString()
        + ", Primary key="
        + this.primaryKey.toString()
        + "]";
  }
}
