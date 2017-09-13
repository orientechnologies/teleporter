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
