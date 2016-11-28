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

import java.util.*;

/**
 * It represents an entity of the source DB.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 *
 */

public class OEntity implements Comparable<OEntity> {

  private String                      name;
  private OSourceDatabaseInfo         sourceDbInfo;
  private String                      schemaName;
  private Set<OAttribute>             attributes;
  private Set<OAttribute>             inheritedAttributes;
  private boolean                     inheritedAttributesRecovered;
  private OPrimaryKey                 primaryKey;
  private List<OForeignKey>           foreignKeys;

  // Canonical relationships
  private Set<OCanonicalRelationship> outCanonicalRelationships;
  private Set<OCanonicalRelationship> inheritedOutCanonicalRelationships;
  private boolean                     inheritedOutCanonicalRelationshipsRecovered;
  private Set<OCanonicalRelationship> inCanonicalRelationships;
  private Set<OCanonicalRelationship> inheritedInCanonicalRelationships;
  private boolean                     inheritedInCanonicalRelationshipsRecovered;

  // Logical relationships
  private Set<OLogicalRelationship>   outLogicalRelationships;
  private Set<OLogicalRelationship>   inLogicalRelationships;

  private Boolean                     isAggregable;
  private String                      directionOfN2NRepresentedRelationship;      // when the entity corresponds to an aggregable
                                                                                  // join table it's 'direct' by default (at the
                                                                                  // first invocation of 'isAggregableJoinTable()')
  private String                      nameOfN2NRepresentedRelationship;           // we can have this parameter only in a join table
                                                                                  // and with the manual migrationConfigDoc of its
                                                                                  // represented relationship
  private OEntity                     parentEntity;
  private int                         inheritanceLevel;
  private OHierarchicalBag            hierarchicalBag;

  public OEntity(String name, String schemaName, OSourceDatabaseInfo sourceDbInfo) {
    this.name = name;
    this.sourceDbInfo = sourceDbInfo;
    this.schemaName = schemaName;
    this.attributes = new LinkedHashSet<OAttribute>();
    this.inheritedAttributes = new LinkedHashSet<OAttribute>();
    this.inheritedAttributesRecovered = false;
    this.foreignKeys = new LinkedList<OForeignKey>();

    // canonical relationships
    this.outCanonicalRelationships = new LinkedHashSet<OCanonicalRelationship>();
    this.inheritedOutCanonicalRelationships = new LinkedHashSet<OCanonicalRelationship>();
    this.inheritedOutCanonicalRelationshipsRecovered = false;
    this.inCanonicalRelationships = new LinkedHashSet<OCanonicalRelationship>();
    this.inheritedInCanonicalRelationships = new LinkedHashSet<OCanonicalRelationship>();
    this.inheritedInCanonicalRelationshipsRecovered = false;

    // logical relationships
    this.outLogicalRelationships = new LinkedHashSet<OLogicalRelationship>();
    this.inLogicalRelationships = new LinkedHashSet<OLogicalRelationship>();

    this.isAggregable = null;
    this.inheritanceLevel = 0;
  }

  /*
   * It's possible to aggregate an entity iff (i) It's a junction (or join) table of dimension 2. (ii) It has not exported keys,
   * that is it's not referenced by other entities.
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

        // if the entity is an aggregable join table then the direction of the N-N represented relationship is set to 'direct' by
        // default.
        if (this.isAggregable && this.directionOfN2NRepresentedRelationship == null) {
          this.directionOfN2NRepresentedRelationship = "direct";
        }

        return this.isAggregable;
      }
    }

  }

  private boolean isJunctionTable() {
    boolean isJunctionTable = true;

    // (i) it's a junction table iff each attribute belonging to the primary key is involved also in a foreign key that imports all
    // the attributes of the primary key of the referenced table.
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
      if (this.getAllInCanonicalRelationships().size() > 0)
        isJunctionTable = false;
    }
    return isJunctionTable;
  }

  public void setIsAggregableJoinTable(boolean isAggregable) {
    this.isAggregable = isAggregable;
  }

  public String getDirectionOfN2NRepresentedRelationship() {
    return this.directionOfN2NRepresentedRelationship;
  }

  public void setDirectionOfN2NRepresentedRelationship(String directionOfN2NRepresentedRelationship) {
    this.directionOfN2NRepresentedRelationship = directionOfN2NRepresentedRelationship;
  }

  public String getNameOfN2NRepresentedRelationship() {
    return this.nameOfN2NRepresentedRelationship;
  }

  public void setNameOfN2NRepresentedRelationship(String nameOfN2NRepresentedRelationship) {
    this.nameOfN2NRepresentedRelationship = nameOfN2NRepresentedRelationship;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public OSourceDatabaseInfo getSourceDataseInfo() {
    return this.sourceDbInfo;
  }

  public void setSourceDbInfo(OSourceDatabaseInfo sourceDbInfo) {
    this.sourceDbInfo = sourceDbInfo;
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

    if (inheritedAttributesRecovered)
      return this.inheritedAttributes;
    else if (parentEntity != null) {
      this.inheritedAttributes = parentEntity.getAllAttributes();
      this.inheritedAttributesRecovered = true;
      return this.inheritedAttributes;
    } else
      return this.inheritedAttributes;
  }

  // Returns attributes and inherited attributes
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

    if (added) {
      Collections.sort(temp);
    }
    this.attributes.clear();
    this.attributes.addAll(temp);
    return added;
  }

  public void removeAttributeByNameIgnoreCase(String toRemove) {

    OAttribute currentAttribute;
    Iterator<OAttribute> it = this.attributes.iterator();
    while (it.hasNext()) {
      currentAttribute = it.next();
      if (currentAttribute.getName().equalsIgnoreCase(toRemove)) {
        it.remove();
        break;
      }
    }
  }

  public OAttribute getAttributeByName(String name) {

    OAttribute toReturn = null;

    for (OAttribute a : this.attributes) {
      if (a.getName().equals(name)) {
        toReturn = a;
        break;
      }
    }
    return toReturn;
  }

  public OAttribute getAttributeByNameIgnoreCase(String name) {

    OAttribute toReturn = null;

    for (OAttribute a : this.attributes) {
      if (a.getName().equalsIgnoreCase(name)) {
        toReturn = a;
        break;
      }
    }
    return toReturn;
  }

  public OAttribute getAttributeByOrdinalPosition(int position) {

    OAttribute toReturn = null;

    for (OAttribute a : this.attributes) {
      if (a.getOrdinalPosition() == position) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  public OAttribute getInheritedAttributeByName(String name) {

    OAttribute toReturn = null;

    for (OAttribute a : this.getInheritedAttributes()) {
      if (a.getName().equals(name)) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  public OAttribute getInheritedAttributeByNameIgnoreCase(String name) {
    OAttribute toReturn = null;

    for (OAttribute a : this.getInheritedAttributes()) {
      if (a.getName().equalsIgnoreCase(name)) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  // Getter and Setter Out Relationships

  public Set<OCanonicalRelationship> getOutCanonicalRelationships() {
    return this.outCanonicalRelationships;
  }

  public void setOutCanonicalRelationships(Set<OCanonicalRelationship> outCanonicalRelationships) {
    this.outCanonicalRelationships = outCanonicalRelationships;
  }

  public Set<OCanonicalRelationship> getInheritedOutCanonicalRelationships() {

    if (inheritedOutCanonicalRelationshipsRecovered)
      return this.inheritedOutCanonicalRelationships;
    else if (parentEntity != null) {
      this.inheritedOutCanonicalRelationships = parentEntity.getAllOutCanonicalRelationships();
      this.inheritedOutCanonicalRelationshipsRecovered = true;
      return this.inheritedOutCanonicalRelationships;
    } else
      return this.inheritedOutCanonicalRelationships;
  }

  public void setInheritedOutCanonicalRelationships(Set<OCanonicalRelationship> inheritedOutCanonicalRelationships) {
    this.inheritedOutCanonicalRelationships = inheritedOutCanonicalRelationships;
  }

  // Returns relationships and inherited relationships (OUT)
  public Set<OCanonicalRelationship> getAllOutCanonicalRelationships() {

    Set<OCanonicalRelationship> allRelationships = new LinkedHashSet<OCanonicalRelationship>();
    allRelationships.addAll(this.getInheritedOutCanonicalRelationships());
    allRelationships.addAll(this.outCanonicalRelationships);

    return allRelationships;
  }

  public boolean isInheritedOutCanonicalRelationshipsRecovered() {
    return inheritedOutCanonicalRelationshipsRecovered;
  }

  public void setInheritedOutCanonicalRelationshipsRecovered(boolean inheritedOutCanonicalRelationshipsRecovered) {
    this.inheritedOutCanonicalRelationshipsRecovered = inheritedOutCanonicalRelationshipsRecovered;
  }

  // Getter and Setter In Relationships

  public Set<OCanonicalRelationship> getInCanonicalRelationships() {
    return this.inCanonicalRelationships;
  }

  public void setInCanonicalRelationships(Set<OCanonicalRelationship> inCanonicalRelationships) {
    this.inCanonicalRelationships = inCanonicalRelationships;
  }

  public Set<OCanonicalRelationship> getInheritedInCanonicalRelationships() {

    if (inheritedInCanonicalRelationshipsRecovered)
      return this.inheritedInCanonicalRelationships;
    else if (parentEntity != null) {
      this.inheritedInCanonicalRelationships = parentEntity.getAllInCanonicalRelationships();
      this.inheritedInCanonicalRelationshipsRecovered = true;
      return this.inheritedInCanonicalRelationships;
    } else
      return this.inheritedInCanonicalRelationships;
  }

  public void setInheritedInCanonicalRelationships(Set<OCanonicalRelationship> inheritedInCanonicalRelationships) {
    this.inheritedInCanonicalRelationships = inheritedInCanonicalRelationships;
  }

  // Returns relationships and inherited relationships (IN)
  public Set<OCanonicalRelationship> getAllInCanonicalRelationships() {

    Set<OCanonicalRelationship> allRelationships = new LinkedHashSet<OCanonicalRelationship>();
    allRelationships.addAll(this.getInheritedInCanonicalRelationships());
    allRelationships.addAll(this.inCanonicalRelationships);

    return allRelationships;
  }

  public boolean isInheritedInCanonicalRelationshipsRecovered() {
    return inheritedInCanonicalRelationshipsRecovered;
  }

  public void setInheritedInCanonicalRelationshipsRecovered(boolean inheritedInCanonicalRelationshipsRecovered) {
    this.inheritedInCanonicalRelationshipsRecovered = inheritedInCanonicalRelationshipsRecovered;
  }

  public Set<OLogicalRelationship> getOutLogicalRelationships() {
    return outLogicalRelationships;
  }

  public void setOutLogicalRelationships(Set<OLogicalRelationship> outLogicalRelationships) {
    this.outLogicalRelationships = outLogicalRelationships;
  }

  public Set<OLogicalRelationship> getInLogicalRelationships() {
    return inLogicalRelationships;
  }

  public void setInLogicalRelationships(Set<OLogicalRelationship> inLogicalRelationships) {
    this.inLogicalRelationships = inLogicalRelationships;
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
    for (OAttribute attribute : this.attributes) {
      attribute.setOrdinalPosition(i);
      i++;
    }
  }

  @Override
  public int compareTo(OEntity toCompare) {

    if (this.inheritanceLevel > toCompare.getInheritanceLevel())
      return 1;
    else if (this.inheritanceLevel < toCompare.getInheritanceLevel())
      return -1;
    else
      return this.name.compareTo(toCompare.getName());

  }

  @Override
  public String toString() {
    String s = "Entity [name = " + this.name + ", number of attributes = " + this.attributes.size() + "]";

    if (this.isAggregableJoinTable())
      s += "\t\t\tJoin Entity (Aggregable Join Table)";

    s += "\n|| ";

    for (OAttribute a : this.attributes)
      s += a.getOrdinalPosition() + ": " + a.getName() + " ( " + a.getDataType() + " ) || ";

    s += "\nPrimary Key (" + this.primaryKey.getInvolvedAttributes().size() + " involved attributes): ";

    int cont = 1;
    int size = this.primaryKey.getInvolvedAttributes().size();
    for (OAttribute a : this.primaryKey.getInvolvedAttributes()) {
      if (cont < size)
        s += a.getName() + ", ";
      else
        s += a.getName() + ".";
      cont++;
    }

    if (this.outCanonicalRelationships.size() > 0) {

      s += "\nForeign Keys (" + outCanonicalRelationships.size() + "):\n";
      int index = 1;

      for (OCanonicalRelationship relationship : this.outCanonicalRelationships) {
        s += index + ".  ";
        s += "Foreign Entity: " + relationship.getForeignEntity().getName() + ", Foreign Key: "
            + relationship.getForeignKey().toString() + "\t||\t" + "Parent Entity: " + relationship.getParentEntity().getName()
            + ", Primary Key: " + relationship.getForeignKey().toString() + "\n";
        index++;
      }

    } else {
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
    return this.name.equals(that.getName()) && this.getSourceDataseInfo().equals(that.getSourceDataseInfo());
  }

}
