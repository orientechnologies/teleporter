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

package com.orientechnologies.orient.teleporter.model.dbschema;

import java.util.ArrayList;
import java.util.List;

/**
 * It represents the schema of a source DB with all its elements.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ODataBaseSchema implements ODataSourceSchema {

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

  public ODataBaseSchema(String productName, String productVersion) {		
    this.productName = productName;
    this.productVersion = productVersion;
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
