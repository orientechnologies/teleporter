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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * It represents a hierarchical tree of entities.
 * It collects the involved entities, the "inheritance strategy" adopted at the lower level (DBMS level) and other
 * meta-data useful for records importing.
 *
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public class OHierarchicalBag {

  private Map<Integer,Set<OEntity>> depth2entities;
  private String inheritancePattern;

  private String discriminatorColumn;
  private Map<String,String> entityName2discriminatorValue;

  public OHierarchicalBag() {
    this.depth2entities = new LinkedHashMap<Integer,Set<OEntity>>();
    this.entityName2discriminatorValue = new HashMap<String,String>();
  }

  public OHierarchicalBag(String inheritancePattern) {
    this.inheritancePattern = inheritancePattern;
    this.depth2entities = new HashMap<Integer,Set<OEntity>>();
    this.entityName2discriminatorValue = new HashMap<String,String>();
  }


  public Map<Integer, Set<OEntity>> getDepth2entities() {
    return this.depth2entities;
  }


  public void setDepth2entities(Map<Integer, Set<OEntity>> depth2entities) {
    this.depth2entities = depth2entities;
  }


  public String getInheritancePattern() {
    return this.inheritancePattern;
  }


  public void setInheritancePattern(String inheritancePattern) {
    this.inheritancePattern = inheritancePattern;
  }


  public String getDiscriminatorColumn() {
    return this.discriminatorColumn;
  }


  public void setDiscriminatorColumn(String discriminatorColumn) {
    this.discriminatorColumn = discriminatorColumn;
  }

  public Map<String, String> getEntityName2discriminatorValue() {
    return this.entityName2discriminatorValue;
  }

  public void setEntityName2discriminatorValue(Map<String, String> entityName2discriminatorValue) {
    this.entityName2discriminatorValue = entityName2discriminatorValue;
  }


  @Override
  public int hashCode() {

    Iterator<OEntity> it = getDepth2entities().get(0).iterator();
    OEntity rootEntity = it.next();

    final int prime = 31;
    int result = 1;
    result = prime * result + ((rootEntity == null) ? 0 : rootEntity.getName().hashCode());
    result = prime * result + ((inheritancePattern == null) ? 0 : inheritancePattern.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {

    OHierarchicalBag that = (OHierarchicalBag) obj;

    Iterator<OEntity> it = this.getDepth2entities().get(0).iterator();
    OEntity rootEntity = it.next();

    it = that.getDepth2entities().get(0).iterator();
    OEntity thatRootEntity = it.next();

    if(this.inheritancePattern.equals(that.getInheritancePattern()) && rootEntity.getName().equals(thatRootEntity.getName())) {
      return true;
    }
    return false;
  }


  public OSourceDatabaseInfo getSourceDataseInfo() {

    Set<OEntity> entities = this.getDepth2entities().get(0);
    Iterator<OEntity> it = entities.iterator();
    if(it.hasNext()) {
      return it.next().getSourceDataseInfo();
    }
    return null;
  }
}
