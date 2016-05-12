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

package com.orientdb.teleporter.model.dbschema;

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
  private String schemaName;
  private Set<OAttribute> attributes;
  private Set<OAttribute> inheritedAttributes;
  private boolean inheritedAttributesRecovered;
  private OPrimaryKey primaryKey;
  private List<OForeignKey> foreignKeys;
  private Set<ORelationship> outRelationships;
  private Set<ORelationship> inheritedOutRelationships;
  private boolean inheritedOutRelationshipsRecovered;
  private Set<ORelationship> inRelationships;
  private Set<ORelationship> inheritedInRelationships;
  private boolean inheritedInRelationshipsRecovered;
  private Boolean isAggregable;
  private OEntity parentEntity;
  private int inheritanceLevel;
  private OHierarchicalBag hierarchicalBag;

  public OEntity(String name, String schemaName) {
    this.name = name;
    this.schemaName = schemaName;
    this.attributes = new LinkedHashSet<OAttribute>();
    this.inheritedAttributes = new LinkedHashSet<OAttribute>();
    this.inheritedAttributesRecovered = false;
    this.foreignKeys = new LinkedList<OForeignKey>();
    this.outRelationships = new LinkedHashSet<ORelationship>();
    this.inheritedOutRelationships = new LinkedHashSet<ORelationship>();
    this.inheritedOutRelationshipsRecovered = false;
    this.inRelationships = new LinkedHashSet<ORelationship>();
    this.inheritedInRelationships = new LinkedHashSet<ORelationship>();
    this.inheritedInRelationshipsRecovered = false;
    this.isAggregable = null;
    this.inheritanceLevel = 0;
  }

  /*
   * It's possible to aggregate an entity iff
   * (i) It's a junction (or join) table of dimension 2.
   * (ii) It has not exported keys, that is it's not referenced by other entities.
   */
  public boolean isAggregableJoinTable() {

    // if already known, just retrieve the info
    if (this.isAggregable != null) {
      return this.isAggregable;
    }

    else {

      // (i) preliminar check
      if (this.foreignKeys.size() != 2)
        return false;

      else {
        boolean aggregable = isJunctionTable();
        this.isAggregable = aggregable;
        return this.isAggregable;
      }
    }

  }

  private boolean isJunctionTable() {
    boolean isJunctionTable = true;

    // (i) it's a junction table iff each attribute belonging to the primary key is involved also in a foreign key that imports all the attributes of the primary key of the referenced table.
    for (OForeignKey currentFk : this.foreignKeys) {
      for (OAttribute attribute : currentFk.getInvolvedAttributes()) {
        if (!this.primaryKey.getInvolvedAttributes().contains(attribute)) {
          isJunctionTable = false;
          break;
        }
      }
    }

    // (ii) check
    if (isJunctionTable) {
      if (this.getAllInRelationships().size() > 0)
        isJunctionTable = false;
    }
    return isJunctionTable;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSchemaName() {
    return this.schemaName;
  }


  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
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

  public OAttribute getAttributeByOrdinalPosition(int position) {

    OAttribute toReturn = null;

    for(OAttribute a: this.attributes) {
      if(a.getOrdinalPosition() == position) {
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

  // Getter and Setter Out Relationships

  public Set<ORelationship> getOutRelationships() {
    return this.outRelationships;
  }

  public void setOutRelationships(Set<ORelationship> outRelationships) {
    this.outRelationships = outRelationships;
  }

  public Set<ORelationship> getInheritedOutRelationships() {

    if(inheritedOutRelationshipsRecovered)
      return this.inheritedOutRelationships;
    else if(parentEntity != null) {
      this.inheritedOutRelationships = parentEntity.getAllOutRelationships();
      this.inheritedOutRelationshipsRecovered = true;
      return this.inheritedOutRelationships;
    }
    else
      return this.inheritedOutRelationships;
  }

  public void setInheritedOutRelationships(Set<ORelationship> inheritedOutRelationships) {
    this.inheritedOutRelationships = inheritedOutRelationships;
  }

  //Returns relationships and inherited relationships (OUT)
  public Set<ORelationship> getAllOutRelationships() {

    Set<ORelationship> allRelationships = new LinkedHashSet<ORelationship>();
    allRelationships.addAll(this.getInheritedOutRelationships());
    allRelationships.addAll(this.outRelationships);

    return allRelationships;
  }

  public boolean isInheritedOutRelationshipsRecovered() {
    return inheritedOutRelationshipsRecovered;
  }

  public void setInheritedOutRelationshipsRecovered(boolean inheritedOutRelationshipsRecovered) {
    this.inheritedOutRelationshipsRecovered = inheritedOutRelationshipsRecovered;
  }


  // Getter and Setter In Relationships

  public Set<ORelationship> getInRelationships() {
    return this.inRelationships;
  }

  public void setInRelationships(Set<ORelationship> inRelationships) {
    this.inRelationships = inRelationships;
  }

  public Set<ORelationship> getInheritedInRelationships() {

    if(inheritedInRelationshipsRecovered)
      return this.inheritedInRelationships;
    else if(parentEntity != null) {
      this.inheritedInRelationships = parentEntity.getAllInRelationships();
      this.inheritedInRelationshipsRecovered = true;
      return this.inheritedInRelationships;
    }
    else
      return this.inheritedInRelationships;
  }

  public void setInheritedInRelationships(Set<ORelationship> inheritedInRelationships) {
    this.inheritedInRelationships = inheritedInRelationships;
  }

  //Returns relationships and inherited relationships (IN)
  public Set<ORelationship> getAllInRelationships() {

    Set<ORelationship> allRelationships = new LinkedHashSet<ORelationship>();
    allRelationships.addAll(this.getInheritedInRelationships());
    allRelationships.addAll(this.inRelationships);

    return allRelationships;
  }

  public boolean isInheritedInRelationshipsRecovered() {
    return inheritedInRelationshipsRecovered;
  }

  public void setInheritedInRelationshipsRecovered(boolean inheritedInRelationshipsRecovered) {
    this.inheritedInRelationshipsRecovered = inheritedInRelationshipsRecovered;
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

    if(this.isAggregableJoinTable())
      s += "\t\t\tJoin Entity (Aggregable Join Table)";

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

    if(this.outRelationships.size() > 0) {

      s += "\nForeign Keys ("+outRelationships.size()+"):\n";
      int index = 1;

      for(ORelationship relationship: this.outRelationships) {
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
