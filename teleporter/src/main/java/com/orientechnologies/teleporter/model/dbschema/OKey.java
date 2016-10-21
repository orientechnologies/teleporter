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

import java.util.LinkedList;
import java.util.List;

/**
 * It represents a generic key of the source DB. It's extended from OPrimaryKey
 * and OForeignKey that differ each other only by their usage.
 * 
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 * 
 */

public class OKey {

  protected OEntity belongingEntity;
  protected List<OAttribute> involvedAttributes;

  public OKey() {
    this.involvedAttributes = new LinkedList<OAttribute>();
  }

  public OKey(OEntity belongingEntity) {
    this.belongingEntity = belongingEntity;
    this.involvedAttributes = new LinkedList<OAttribute>();
  }

  public OEntity getBelongingEntity() {
    return this.belongingEntity;
  }

  public void setBelongingEntity(OEntity belongingEntity) {
    this.belongingEntity = belongingEntity;
  }

  public List<OAttribute> getInvolvedAttributes() {
    return this.involvedAttributes;
  }

  public void setInvolvedAttributes(List<OAttribute> involvedAttributes) {
    this.involvedAttributes = involvedAttributes;
  }

  public void addAttribute(OAttribute attribute) {
    this.involvedAttributes.add(attribute);
  }

  public boolean removeAttribute(OAttribute toRemove) {
    return this.involvedAttributes.remove(toRemove);
  }

  public OAttribute getAttributeByName(String name) {

    OAttribute toReturn = null;

    for(OAttribute a: this.involvedAttributes) {
      if(a.getName().equals(name)) {
        toReturn = a;
        break;
      }
    }
    return toReturn;
  }

  public OAttribute getAttributeByNameIgnoreCase(String name) {

    OAttribute toReturn = null;

    for(OAttribute a: this.involvedAttributes) {
      if(a.getName().equalsIgnoreCase(name)) {
        toReturn = a;
        break;
      }
    }
    return toReturn;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((belongingEntity == null) ? 0 : belongingEntity.getName().hashCode());
    result = prime * result + ((involvedAttributes == null) ? 0 : involvedAttributes.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    OKey that = (OKey) obj;

    if(this.belongingEntity.getName().equals(that.belongingEntity.getName())) {
      if(this.involvedAttributes.equals(that.getInvolvedAttributes())) {
        return true;
      }
    }

    return false;
  }

  public String toString() {
    String s = "[";

    for(OAttribute attribute: this.involvedAttributes) {
      s += attribute.getName() + ",";
    }

    s = s.substring(0, s.length()-1);

    s += "]";

    return s;
  }



}
