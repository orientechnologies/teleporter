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

import com.orientechnologies.teleporter.model.ODataSourceSchemaInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * It represents the schema of a source DB with all its elements.
 * 
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 * 
 */

public class ODataBaseSchema implements ODataSourceSchemaInfo {

  private int majorVersion;
  private int minorVersion;	
  private int driverMajorVersion;
  private int driverMinorVersion;
  private String productName;
  private String productVersion;
  private List<OEntity> entities;
  private List<ORelationship> relationships;
  private List<OHierarchicalBag> hierarchicalBags;

  public ODataBaseSchema(int majorVersion, int minorVersion, int driverMajorVersion, int driverMinorVersion, String productName, String productVersion) {		
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    this.driverMajorVersion = driverMajorVersion;
    this.driverMinorVersion = driverMinorVersion;
    this.productName = productName;
    this.productVersion = productVersion;
    this.entities = new ArrayList<OEntity>();
    this.relationships = new ArrayList<ORelationship>();
    this.hierarchicalBags = new ArrayList<OHierarchicalBag>();
  }

  public ODataBaseSchema() {
    this.entities = new ArrayList<OEntity>();
    this.relationships = new ArrayList<ORelationship>();
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

  public List<ORelationship> getRelationships() {
    return relationships;
  }

  public void setRelationships(List<ORelationship> relationships) {
    this.relationships = relationships;
  }

  public List<OHierarchicalBag> getHierarchicalBags() {
    return hierarchicalBags;
  }

  public void setHierarchicalBags(List<OHierarchicalBag> hierarchicalBags) {
    this.hierarchicalBags = hierarchicalBags;
  }

  public OEntity getEntityByName(String entityName) {

    for(OEntity currentEntity: this.entities) {
      if(currentEntity.getName().equals(entityName))
        return currentEntity;
    }

    return null;
  }

  public OEntity getEntityByNameIgnoreCase(String entityName) {

    for(OEntity currentEntity: this.entities) {
      if(currentEntity.getName().equalsIgnoreCase(entityName))
        return currentEntity;
    }

    return null;
  }

  public ORelationship getRelationshipByInvolvedEntitiesAndAttributes(OEntity currentForeignEntity, OEntity currentParentEntity,
      List<String> fromColumns, List<String> toColumns) {

    for(ORelationship currentRelationship: this.relationships) {
      if(currentRelationship.getForeignEntity().getName().equals(currentForeignEntity.getName()) && currentRelationship.getParentEntity().getName().equals(currentParentEntity.getName())) {
        if(sameAttributesInvolved(currentRelationship.getForeignKey(), fromColumns) && sameAttributesInvolved(currentRelationship.getPrimaryKey(), toColumns)) {
          return currentRelationship;
        }
      }
    }
    return null;
  }

  /**
   * It checks if the attributes of a OKey passed as parameter correspond to the string names in the array columns.
   * Order is not relevant.
   *
   * @param key
   * @param columns
   * @return
   */
  private boolean sameAttributesInvolved(OKey key, List<String> columns) {

    if(key.getInvolvedAttributes().size() != columns.size()) {
      return false;
    }

    for(String column: columns) {
      if(key.getAttributeByName(column) == null) {
        return false;
      }
    }

    for(String column: columns) {
      if(key.getAttributeByName(column) == null) {
        return false;
      }
    }

    return true;
  }

  public String toString() {
    String s = "\n\n\n------------------------------ DB SCHEMA DESCRIPTION ------------------------------\n\n" + 
        "\nProduct name: " + this.productName + "\tProduct version: " + this.productVersion +
        "\nMajor version: " + this.majorVersion + "\tMinor Version: " + this.minorVersion + 
        "\nDriver major version: " + this.driverMajorVersion + "\tDriver minor version: " + this.driverMinorVersion + "\n\n\n";

    s += "Number of Entities: " + this.entities.size() + ".\n"
        + "Number of Relationship: " + this.relationships.size() + ".\n\n\n";

    for(OEntity e: this.entities)
      s += e.toString();
    return s;		
  }

}
