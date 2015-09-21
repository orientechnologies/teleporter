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

package com.orientechnologies.teleporter.nameresolver;

import com.orientechnologies.teleporter.model.dbschema.ORelationship;

/**
 * Implementation of ONameResolver that performs name transformations on the elements 
 * of the data source according to the Java convention.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OJavaConventionNameResolver implements ONameResolver {

  @Override
  public String resolveVertexName(String candidateName) {

    // manipulating name (Java Convention)
    candidateName = this.toJavaClassConvention(candidateName);

    return candidateName;
  }

  @Override
  public String resolveVertexProperty(String candidateName) {

    // manipulating name (Java Convention)
    candidateName = this.toJavaVariableConvention(candidateName);

    return candidateName;
  }  

  @Override
  public String resolveEdgeName(ORelationship relationship) {
    String finalName;

    // Foreign Key composed of 1 attribute
    if(relationship.getForeignKey().getInvolvedAttributes().size() == 1) {
      String columnName = relationship.getForeignKey().getInvolvedAttributes().get(0).getName();
      columnName = columnName.replace("_id", "");
      columnName = columnName.replace("_ID", "");
      columnName = columnName.replace("_oid", "");
      columnName = columnName.replace("_OID", "");
      columnName = columnName.replace("_eid", "");
      columnName = columnName.replace("_EID", "");


      // manipulating name (Java Convention)
      columnName = this.toJavaClassConvention(columnName);
      finalName = "Has" + columnName;
    }

    // Foreign Key composed of multiple attribute
    else {         
      finalName = this.toJavaClassConvention(relationship.getForeignEntityName()) + "2" + this.toJavaClassConvention(relationship.getParentEntityName());
    }

    return finalName;
  }



  protected String toJavaClassConvention(String name) {

    name = name.toLowerCase();
    name = (name.charAt(0)+"").toUpperCase() + name.substring(1);  // First char to upper case

    if(name.contains("_")) {
      int pos;
      while(name.contains("_")) {
        pos = name.indexOf("_");
        name = name.substring(0,pos) + (name.charAt(pos+1)+"").toUpperCase() + name.substring(pos+2);
      }
    }

    if(name.contains("-")) {
      int pos;
      while(name.contains("-")) {
        pos = name.indexOf("-");
        name = name.substring(0,pos) + (name.charAt(pos+1)+"").toUpperCase() + name.substring(pos+2);
      }
    }

    if(name.contains("_")) {
      int pos;
      while(name.contains("_")) {
        pos = name.indexOf("_");
        name = name.substring(0,pos) + (name.charAt(pos+1)+"").toUpperCase() + name.substring(pos+2);
      }
    }
    return name;
  }


  private String toJavaVariableConvention(String name) {

    name = name.toLowerCase();

    if(name.contains("_")) {
      int pos;
      while(name.contains("_")) {
        pos = name.indexOf("_");
        name = name.substring(0,pos) + (name.charAt(pos+1)+"").toUpperCase() + name.substring(pos+2);
      }
    }

    if(name.contains("-")) {
      int pos;
      while(name.contains("-")) {
        pos = name.indexOf("-");
        name = name.substring(0,pos) + (name.charAt(pos+1)+"").toUpperCase() + name.substring(pos+2);
      }
    }

    if(name.contains("_")) {
      int pos;
      while(name.contains("_")) {
        pos = name.indexOf("_");
        name = name.substring(0,pos) + (name.charAt(pos+1)+"").toUpperCase() + name.substring(pos+2);
      }
    }
    return name;
  }


  @Override
  public String reverseTransformation(String transformedName) {

    for(int i=0; i<transformedName.length(); i++) {
      if(i == 0 && Character.isUpperCase(transformedName.charAt(i))) {
        transformedName = transformedName.substring(0, i) + (transformedName.charAt(i)+"").toLowerCase() + transformedName.substring(i+1);      
      }
      if(Character.isUpperCase(transformedName.charAt(i))) {
        transformedName = transformedName.substring(0, i) + "_" + (transformedName.charAt(i)+"").toLowerCase() + transformedName.substring(i+1);        
      }
    }
    return transformedName;

  }
  
}
