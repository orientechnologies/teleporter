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

import com.orientechnologies.teleporter.model.ODataSourceSchemaInfo;
import java.util.ArrayList;
import java.util.List;

/**
 * It represents the schema of a source DB with all its elements.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class ODataBaseSchema implements ODataSourceSchemaInfo {

  private int majorVersion;
  private int minorVersion;
  private int driverMajorVersion;
  private int driverMinorVersion;
  private String productName;
  private String productVersion;
  private List<OEntity> entities;
  private List<OCanonicalRelationship> canonicalRelationships;
  private List<OLogicalRelationship> logicalRelationships;
  private List<OHierarchicalBag> hierarchicalBags;

  public ODataBaseSchema(
      int majorVersion,
      int minorVersion,
      int driverMajorVersion,
      int driverMinorVersion,
      String productName,
      String productVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    this.driverMajorVersion = driverMajorVersion;
    this.driverMinorVersion = driverMinorVersion;
    this.productName = productName;
    this.productVersion = productVersion;
    this.entities = new ArrayList<OEntity>();
    this.canonicalRelationships = new ArrayList<OCanonicalRelationship>();
    this.logicalRelationships = new ArrayList<OLogicalRelationship>();
    this.hierarchicalBags = new ArrayList<OHierarchicalBag>();
  }

  public ODataBaseSchema() {
    this.entities = new ArrayList<OEntity>();
    this.canonicalRelationships = new ArrayList<OCanonicalRelationship>();
    this.logicalRelationships = new ArrayList<OLogicalRelationship>();
    this.hierarchicalBags = new ArrayList<OHierarchicalBag>();
  }

  public int getMajorVersion() {
    return majorVersion;
  }

  public void setMajorVersion(int majorVersion) {
    this.majorVersion = majorVersion;
  }

  public int getMinorVersion() {
    return minorVersion;
  }

  public void setMinorVersion(int minorVersion) {
    this.minorVersion = minorVersion;
  }

  public int getDriverMajorVersion() {
    return driverMajorVersion;
  }

  public void setDriverMajorVersion(int driverMajorVersion) {
    this.driverMajorVersion = driverMajorVersion;
  }

  public int getDriverMinorVersion() {
    return driverMinorVersion;
  }

  public void setDriverMinorVersion(int driverMinorVersion) {
    this.driverMinorVersion = driverMinorVersion;
  }

  public String getProductName() {
    return productName;
  }

  public void setProductName(String productName) {
    this.productName = productName;
  }

  public String getProductVersion() {
    return productVersion;
  }

  public void setProductVersion(String productVersion) {
    this.productVersion = productVersion;
  }

  public List<OEntity> getEntities() {
    return entities;
  }

  public void setEntities(List<OEntity> entitiess) {
    this.entities = entitiess;
  }

  public List<OCanonicalRelationship> getCanonicalRelationships() {
    return canonicalRelationships;
  }

  public void setCanonicalRelationships(List<OCanonicalRelationship> canonicalRelationships) {
    this.canonicalRelationships = canonicalRelationships;
  }

  public List<OLogicalRelationship> getLogicalRelationships() {
    return logicalRelationships;
  }

  public void setLogicalRelationships(List<OLogicalRelationship> logicalRelationships) {
    this.logicalRelationships = logicalRelationships;
  }

  public List<OHierarchicalBag> getHierarchicalBags() {
    return hierarchicalBags;
  }

  public void setHierarchicalBags(List<OHierarchicalBag> hierarchicalBags) {
    this.hierarchicalBags = hierarchicalBags;
  }

  public OEntity getEntityByName(String entityName) {

    for (OEntity currentEntity : this.entities) {
      if (currentEntity.getName().equals(entityName)) return currentEntity;
    }

    return null;
  }

  public OEntity getEntityByNameIgnoreCase(String entityName) {

    for (OEntity currentEntity : this.entities) {
      if (currentEntity.getName().equalsIgnoreCase(entityName)) return currentEntity;
    }

    return null;
  }

  public ORelationship getRelationshipByInvolvedEntitiesAndAttributes(
      OEntity currentForeignEntity,
      OEntity currentParentEntity,
      List<String> fromColumns,
      List<String> toColumns) {

    for (ORelationship currentRelationship : this.canonicalRelationships) {
      if (currentRelationship.getForeignEntity().getName().equals(currentForeignEntity.getName())
          && currentRelationship
              .getParentEntity()
              .getName()
              .equals(currentParentEntity.getName())) {
        if (sameAttributesInvolved(currentRelationship.getFromColumns(), fromColumns)
            && sameAttributesInvolved(currentRelationship.getToColumns(), toColumns)) {
          return currentRelationship;
        }
      }
    }
    return null;
  }

  /**
   * It checks if the attributes of a OKey passed as parameter correspond to the string names in the
   * array columns. Order is not relevant.
   *
   * @param columns
   * @param columnsName
   * @return
   */
  private boolean sameAttributesInvolved(List<OAttribute> columns, List<String> columnsName) {

    if (columns.size() != columnsName.size()) {
      return false;
    }

    for (String column : columnsName) {

      boolean present = false;
      for (OAttribute attribute : columns) {
        if (attribute.getName().equals(column)) {
          present = true;
          break;
        }
      }
      if (!present) {
        return false;
      }
    }

    return true;
  }

  public String toString() {
    String s =
        "\n\n\n------------------------------ DB SCHEMA DESCRIPTION ------------------------------\n\n"
            + "\nProduct name: "
            + this.productName
            + "\tProduct version: "
            + this.productVersion
            + "\nMajor version: "
            + this.majorVersion
            + "\tMinor Version: "
            + this.minorVersion
            + "\nDriver major version: "
            + this.driverMajorVersion
            + "\tDriver minor version: "
            + this.driverMinorVersion
            + "\n\n\n";

    s +=
        "Number of Entities: "
            + this.entities.size()
            + ".\n"
            + "Number of Relationship: "
            + this.canonicalRelationships.size()
            + ".\n\n\n";

    for (OEntity e : this.entities) s += e.toString();
    return s;
  }
}
