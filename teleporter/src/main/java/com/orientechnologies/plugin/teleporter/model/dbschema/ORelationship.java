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

package com.orientechnologies.plugin.teleporter.model.dbschema;

/**
 * It represents the relationship between two entities (foreign and parent entity)
 * based on the importing of a single primary key (composite or not) through a foreign key.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ORelationship {

  private String foreignEntityName;				// Entity importing the key (starting entity)
  private String parentEntityName;				// Entity exporting the key (arrival entity)
  private OForeignKey foreignKey;
  private OPrimaryKey primaryKey;
  
  public ORelationship(String foreignEntityName, String parentEntityName) {
    this.foreignEntityName = foreignEntityName;
    this.parentEntityName = parentEntityName;
  }
  
  public ORelationship(String foreignEntityName, String parentEntityName, OForeignKey foreignKey, OPrimaryKey primaryKey) {
    this.foreignEntityName = foreignEntityName;
    this.parentEntityName = parentEntityName;
    this.foreignKey = foreignKey;
    this.primaryKey = primaryKey;
  }
 
  public String getForeignEntityName() {
    return this.foreignEntityName;
  }

  public void setForeignEntityName(String foreignEntityName) {
    this.foreignEntityName = foreignEntityName;
  }

  public String getParentEntityName() {
    return this.parentEntityName;
  }

  public void setParentEntityName(String parentEntityName) {
    this.parentEntityName = parentEntityName;
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
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((foreignEntityName == null) ? 0 : foreignEntityName.hashCode());
    result = prime * result + ((parentEntityName == null) ? 0 : parentEntityName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    ORelationship that = (ORelationship) obj;
    if(this.foreignEntityName.equals(that.getForeignEntityName()) && this.parentEntityName.equals(that.getParentEntityName())) {
      if(this.foreignKey.equals(that.getForeignKey()) && this.primaryKey.equals(that.getPrimaryKey())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "ORelationship [foreignEntityName=" + foreignEntityName + ", parentEntityName=" + parentEntityName
        + ", Foreign key=" + this.foreignKey.toString() + ", Primary key=" + this.primaryKey.toString() + "]";
  }



}
