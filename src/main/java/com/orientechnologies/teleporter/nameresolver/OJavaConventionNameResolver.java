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

package com.orientechnologies.teleporter.nameresolver;

import com.orientechnologies.teleporter.model.dbschema.OCanonicalRelationship;

import java.util.Locale;

/**
 * Implementation of ONameResolver that performs name transformations on the elements
 * of the data source according to the Java convention.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OJavaConventionNameResolver implements ONameResolver {

  @Override
  public String resolveVertexName(String candidateName) {

    if (this.isCompliantToJavaClassConvention(candidateName))
      return candidateName;

    else {

      // manipulating name (Java Convention)
      candidateName = this.toJavaClassConvention(candidateName);

      return candidateName;
    }
  }

  @Override
  public String resolveVertexProperty(String candidateName) {

    if (this.isCompliantToJavaVariableConvention(candidateName))
      return candidateName;

    else {

      // manipulating name (Java Convention)
      candidateName = this.toJavaVariableConvention(candidateName);

      return candidateName;
    }
  }

  @Override
  public String resolveEdgeName(OCanonicalRelationship relationship) {

    String finalName;

    // Foreign Key composed of 1 attribute
    if (relationship.getFromColumns().size() == 1) {
      String columnName = relationship.getFromColumns().get(0).getName();
      columnName = columnName.replace("_id", "");
      columnName = columnName.replace("_ID", "");
      columnName = columnName.replace("_oid", "");
      columnName = columnName.replace("_OID", "");
      columnName = columnName.replace("_eid", "");
      columnName = columnName.replace("_EID", "");

      if (!this.isCompliantToJavaClassConvention(columnName)) {
        // manipulating name (Java Convention)
        columnName = this.toJavaClassConvention(columnName);
      }

      finalName = "Has" + columnName;
    }

    // Foreign Key composed of multiple attribute
    else {
      finalName = this.toJavaClassConvention(relationship.getForeignEntity().getName()) + "2" + this
          .toJavaClassConvention(relationship.getParentEntity().getName());
    }

    return finalName;

  }

  public String toJavaClassConvention(String name) {

    // if all chars are uppercase, then name is transformed in a lowercase version

    boolean allUpperCase = true;
    for (int i = 0; i < name.length(); i++) {
      if (Character.isLowerCase(name.charAt(i))) {
        allUpperCase = false;
        break;
      }
    }

    if (allUpperCase) {
      name = name.toLowerCase(Locale.ENGLISH);
    }

    if (name.contains(" ")) {
      int pos;
      while (name.contains(" ")) {
        pos = name.indexOf(" ");
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    if (name.contains("_")) {
      int pos;
      while (name.contains("_")) {
        pos = name.indexOf("_");
        if(pos < name.length()-1) {
          // the '_' char is not in last position
          name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
        }
        else {
          // the '_' char is in last position
          name = name.substring(0,name.length()-1);
        }
      }
    }

    if (name.contains("-")) {
      int pos;
      while (name.contains("-")) {
        pos = name.indexOf("-");
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    // First char must be uppercase
    if (Character.isLowerCase(name.charAt(0)))
      name = name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1);

    return name;

  }

  public String toJavaVariableConvention(String name) {

    // if all chars are uppercase, then name is transformed in a lowercase version

    boolean allUpperCase = true;
    for (int i = 0; i < name.length(); i++) {
      if (Character.isLowerCase(name.charAt(i))) {
        allUpperCase = false;
        break;
      }
    }

    if (allUpperCase) {
      name = name.toLowerCase(Locale.ENGLISH);
    }

    if (name.contains(" ")) {
      int pos;
      while (name.contains(" ")) {
        pos = name.indexOf(" ");
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    if (name.contains("_")) {
      int pos;
      while (name.contains("_")) {
        pos = name.indexOf("_");
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    if (name.contains("-")) {
      int pos;
      while (name.contains("-")) {
        pos = name.indexOf("-");
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    // First char must be lowercase
    if (Character.isUpperCase(name.charAt(0)))
      name = name.substring(0, 1).toLowerCase(Locale.ENGLISH) + name.substring(1);

    return name;
  }

  public boolean isCompliantToJavaClassConvention(String candidateName) {

    if (!(candidateName.contains(" ") || candidateName.contains("_") || candidateName.contains("-")) && Character
        .isUpperCase(candidateName.charAt(0))) {

      // if all chars are uppercase, then name is transformed in a lowercase version

      boolean allUpperCase = true;
      for (int i = 0; i < candidateName.length(); i++) {
        if (Character.isLowerCase(candidateName.charAt(i))) {
          allUpperCase = false;
          break;
        }
      }

      if (allUpperCase)
        return false;
      else
        return true;
    } else
      return false;
  }

  public boolean isCompliantToJavaVariableConvention(String candidateName) {

    if (!(candidateName.contains(" ") || candidateName.contains("_") || candidateName.contains("-")) && Character
        .isLowerCase(candidateName.charAt(0)))
      return true;
    else
      return false;

  }

}
