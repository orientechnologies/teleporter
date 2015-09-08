/*
 * Copyright 2015 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.teleporter.model.dbschema;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * It represents an entity of the source DB.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class OEntity implements Comparable<OEntity> {

  private String name;
  private Set<OAttribute> attributes;
  private Set<OAttribute> inheritedAttributes;
  private boolean inheritedAttributesRecovered;
  private OPrimaryKey primaryKey;
  private List<OForeignKey> foreignKeys;
  private Set<ORelationship> relationships;
  private Set<ORelationship> inheritedRelationships;
  private boolean inheritedRelationshipsRecovered;
  private Boolean isJoinEntityDim2;
  private OEntity parentEntity;
  private int inheritanceLevel;
  private OHierarchicalBag hierarchicalBag;

  public OEntity(String name) {
    this.name = name;
    this.attributes = new LinkedHashSet<OAttribute>();
    this.inheritedAttributes = new LinkedHashSet<OAttribute>();
    this.inheritedAttributesRecovered = false;
    this.foreignKeys = new LinkedList<OForeignKey>();
    this.relationships = new LinkedHashSet<ORelationship>();
    this.inheritedRelationships = new LinkedHashSet<ORelationship>();
    this.inheritedRelationshipsRecovered = false;
    this.isJoinEntityDim2 = null;
    this.inheritanceLevel = 0;
  }

  /*
   * It's possible to aggregate an entity if it's a junction (or join) table of dimension 2.
   */
  public boolean isJoinEntityDim2() {

    if(this.foreignKeys.size() != 2)
      return false;

    if(this.isJoinEntityDim2 == null) {

      for(OForeignKey currentFk: this.foreignKeys) {
        for(OAttribute attribute: currentFk.getInvolvedAttributes()) {
          if(!this.primaryKey.getInvolvedAttributes().contains(attribute)) {
            return this.isJoinEntityDim2 = false;
          }
        }
      }
      return this.isJoinEntityDim2 = true;
    }
    else {
      return this.isJoinEntityDim2;
    }

  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }


  public Set<OAttribute> getAttributes() {
    return this.attributes;
  }

  public void setAttributes(Set<OAttribute> attributes) {
    this.attributes = attributes;
  }

  public Set<OAttribute> getInheritedAttributes() {

    if(inheritedAttributesRecovered)
      return this.inheritedAttributes;
    else if(parentEntity != null) {
      this.inheritedAttributes = parentEntity.getAllAttributes();
      this.inheritedAttributesRecovered = true;
      return this.inheritedAttributes;
    }
    else
      return this.inheritedAttributes;
  }
  
  //Returns attributes and inherited attributes
  public Set<OAttribute> getAllAttributes() {

    Set<OAttribute> allAttributes = new LinkedHashSet<OAttribute>();
    allAttributes.addAll(this.getInheritedAttributes());
    allAttributes.addAll(this.attributes);

    return allAttributes;
  }  

  public void setInheritedAttributes(Set<OAttribute> inheritedAttributes) {
    this.inheritedAttributes = inheritedAttributes;
  }

  public boolean isInheritedAttributesRecovered() {
    return inheritedAttributesRecovered;
  }

  public void setInheritedAttributesRecovered(boolean inheritedAttributesRecovered) {
    this.inheritedAttributesRecovered = inheritedAttributesRecovered;
  }

  public OPrimaryKey getPrimaryKey() {
    return this.primaryKey;
  }

  public void setPrimaryKey(OPrimaryKey primaryKey) {
    this.primaryKey = primaryKey;
  }

  public List<OForeignKey> getForeignKeys() {
    return foreignKeys;
  }

  public void setForeignKeys(List<OForeignKey> foreignKeys) {
    this.foreignKeys = foreignKeys;
  }

  public boolean addAttribute(OAttribute attribute) {
    boolean added = this.attributes.add(attribute);
    List<OAttribute> temp = new LinkedList<OAttribute>(this.attributes);

    if(added) {
      Collections.sort(temp);
    }
    this.attributes.clear();
    this.attributes.addAll(temp);
    return added;
  }

  public void removeAttributeByNameIgnoreCase(String toRemove) {

    OAttribute currentAttribute;
    Iterator<OAttribute> it = this.attributes.iterator();
    while(it.hasNext()) {
      currentAttribute = it.next();
      if(currentAttribute.getName().equalsIgnoreCase(toRemove)) {
        it.remove();
        break;
      }
    }
  }


  public OAttribute getAttributeByName(String name) {

    OAttribute toReturn = null;

    for(OAttribute a: this.attributes) {
      if(a.getName().equals(name)) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  public OAttribute getAttributeByNameIgnoreCase(String name) {
    OAttribute toReturn = null;

    for(OAttribute a: this.attributes) {
      if(a.getName().equalsIgnoreCase(name)) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  public OAttribute getInheritedAttributeByName(String name) {

    OAttribute toReturn = null;

    for(OAttribute a: this.getInheritedAttributes()) {
      if(a.getName().equals(name)) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  public OAttribute getInheritedAttributeByNameIgnoreCase(String name) {
    OAttribute toReturn = null;

    for(OAttribute a: this.getInheritedAttributes()) {
      if(a.getName().equalsIgnoreCase(name)) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  public Set<ORelationship> getRelationships() {
    return this.relationships;
  }

  public void setRelationships(Set<ORelationship> relationships) {
    this.relationships = relationships;
  }

  public Set<ORelationship> getInheritedRelationships() {

    if(inheritedRelationshipsRecovered)
      return this.inheritedRelationships;
    else if(parentEntity != null) {
      this.inheritedRelationships = parentEntity.getAllRelationships();
      this.inheritedRelationshipsRecovered = true;
      return this.inheritedRelationships;
    }
    else
      return this.inheritedRelationships; 
  }

  public void setInheritedRelationships(Set<ORelationship> inheritedRelationships) {
    this.inheritedRelationships = inheritedRelationships;
  }

  //Returns relationships and inherited relationships
  public Set<ORelationship> getAllRelationships() {

    Set<ORelationship> allRelationships = new LinkedHashSet<ORelationship>();
    allRelationships.addAll(this.getInheritedRelationships());
    allRelationships.addAll(this.relationships);

    return allRelationships;
  }

  public boolean isInheritedRelationshipsRecovered() {
    return inheritedRelationshipsRecovered;
  }

  public void setInheritedRelationshipsRecovered(boolean inheritedRelationshipsRecovered) {
    this.inheritedRelationshipsRecovered = inheritedRelationshipsRecovered;
  }

  public OEntity getParentEntity() {
    return this.parentEntity;
  }

  public void setParentEntity(OEntity parentEntity) {
    this.parentEntity = parentEntity;
  }

  public int getInheritanceLevel() {
    return this.inheritanceLevel;
  }

  public void setInheritanceLevel(int inheritanceLevel) {
    this.inheritanceLevel = inheritanceLevel;
  }

  public OHierarchicalBag getHierarchicalBag() {
    return hierarchicalBag;
  }

  public void setHierarchicalBag(OHierarchicalBag hierarchicalBag) {
    this.hierarchicalBag = hierarchicalBag;
  }

  public void renumberAttributesOrdinalPositions() {
    int i = 1;
    for(OAttribute attribute: this.attributes) {
      attribute.setOrdinalPosition(i);
      i++;
    }
  }

  @Override
  public int compareTo(OEntity toCompare) {

    if(this.inheritanceLevel > toCompare.getInheritanceLevel())
      return 1;
    else if(this.inheritanceLevel < toCompare.getInheritanceLevel())
      return -1;
    else
      return this.name.compareTo(toCompare.getName());

  }

  @Override
  public String toString() {
    String s = "Entity [name = " + this.name + ", number of attributes = " + this.attributes.size() + "]";	

    if(this.isJoinEntityDim2())
      s += "\t\t\tJoin Entity (Join Table of dimension 2)";

    s += "\n|| ";

    for(OAttribute a: this.attributes)
      s += a.getOrdinalPosition() + ": " + a.getName() + " ( " + a.getDataType() + " ) || ";

    s += "\nPrimary Key (" + this.primaryKey.getInvolvedAttributes().size() + " involved attributes): ";

    int cont = 1;
    int size = this.primaryKey.getInvolvedAttributes().size();
    for(OAttribute a: this.primaryKey.getInvolvedAttributes()) {
      if(cont < size)
        s += a.getName()+", ";
      else
        s += a.getName()+".";
      cont++;
    }

    if(this.relationships.size() > 0) {

      s += "\nForeign Keys ("+relationships.size()+"):\n";
      int index = 1;

      for(ORelationship relationship: this.relationships) {
        s += index +".  ";
        s += "Foreign Entity: " + relationship.getForeignEntityName() + ", Foreign Key: " + relationship.getForeignKey().toString() + "\t||\t" 
            + "Parent Entity: " + relationship.getParentEntityName() + ", Primary Key: " + relationship.getForeignKey().toString() + "\n";
        index++;
      }

    }
    else {
      s += "\nForeign Key: Not Present\n";
    }

    s += "\n\n";
    return s;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    OEntity that = (OEntity) obj;
    return this.name.equals(that.getName());

  }


}
