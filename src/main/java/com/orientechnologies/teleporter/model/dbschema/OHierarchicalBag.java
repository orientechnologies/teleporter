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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * It represents a hierarchical tree of entities. It collects the involved entities, the
 * "inheritance strategy" adopted at the lower level (DBMS level) and other meta-data useful for
 * records importing.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OHierarchicalBag {

  private Map<Integer, Set<OEntity>> depth2entities;
  private String inheritancePattern;

  private String discriminatorColumn;
  private Map<String, String> entityName2discriminatorValue;

  public OHierarchicalBag() {
    this.depth2entities = new LinkedHashMap<Integer, Set<OEntity>>();
    this.entityName2discriminatorValue = new HashMap<String, String>();
  }

  public OHierarchicalBag(String inheritancePattern) {
    this.inheritancePattern = inheritancePattern;
    this.depth2entities = new HashMap<Integer, Set<OEntity>>();
    this.entityName2discriminatorValue = new HashMap<String, String>();
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

    if (this.inheritancePattern.equals(that.getInheritancePattern())
        && rootEntity.getName().equals(thatRootEntity.getName())) {
      return true;
    }
    return false;
  }

  public OSourceDatabaseInfo getSourceDataseInfo() {

    Set<OEntity> entities = this.getDepth2entities().get(0);
    Iterator<OEntity> it = entities.iterator();
    if (it.hasNext()) {
      return it.next().getSourceDataseInfo();
    }
    return null;
  }
}
