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

package com.orientechnologies.orient.importer.model.dbschema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * <CLASS DESCRIPTION>
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 * 
 */

public class OEntity {


  private String name;
  private List<OAttribute> attributes;
  private OPrimaryKey primaryKey;
  private List<OForeignKey> foreignKeys;
  private List<ORelationship> relationships;
  private Boolean isJunctionEntity;

  public OEntity(String name) {
    this.name = name;
    this.attributes = new LinkedList<OAttribute>();
    this.foreignKeys = new LinkedList<OForeignKey>();
    this.relationships = new ArrayList<ORelationship>();
    this.isJunctionEntity = null;
  }

  public boolean isJunctionEntity() {

    if(this.isJunctionEntity == null) {

      for(OAttribute attribute: this.primaryKey.getInvolvedAttributes()) {         
        if(!this.isInvolvedInForeignKeys(attribute)) {
          return false;
        }      
      }
      return true; 
    }
    else {
      return this.isJunctionEntity();
    }

  }

  private boolean isInvolvedInForeignKeys(OAttribute attribute) {

    for(ORelationship relationship: this.relationships) {
      for(String fkAttribute: relationship.getForeignAttributes2parentAttributes().keySet()) {
        if(fkAttribute.equals(attribute.getName())) {
          return true;
        }
      }
    }      
    return false;    
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }


  public List<OAttribute> getAttributes() {
    return this.attributes;
  }

  public void setAttributes(List<OAttribute> attributes) {
    this.attributes = attributes;
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

//  public void addAttribute(OAttribute attribute) {
//    this.attributes.add(attribute);
//  }
  
  public boolean addAttribute(OAttribute attribute) {
    boolean added = this.attributes.add(attribute);
    
    if(added) {
      Collections.sort(this.attributes);
    }
    return added;
  }

  public boolean removeAttribute(OAttribute toRemove) {
    return this.attributes.remove(toRemove);
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

  public List<ORelationship> getRelationships() {
    return this.relationships;
  }

  public void setRelationships(List<ORelationship> relationships) {
    this.relationships = relationships;
  }

  @Override
  public String toString() {
    String s = "Entity [name = " + this.name + ", number of attributes = " + this.attributes.size() + "]";	

    if(this.isJunctionEntity())
      s += "\t\t\tJunction Entity (Join Table)";

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
            + "Parent Entity: " + relationship.getParentEntityName() + ", Primary Key: " + relationship.getPrimaryKey().toString() + "\n";
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
