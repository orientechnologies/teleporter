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

package com.orientechnologies.orient.importer.nameresolver.javaconvention;

import com.orientechnologies.orient.importer.mapper.OER2GraphMapper;
import com.orientechnologies.orient.importer.model.dbschema.ORelationship;
import com.orientechnologies.orient.importer.nameresolver.ONameResolver;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 *
 */

public class OJavaConventionNumberedEdge implements ONameResolver {


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
  public String resolveEdgeName(ORelationship relationship, OER2GraphMapper mapper) {
    String candidateName = null;
    String finalName = null;

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

      // check presence of class vertex or edge called with the "candidate-name"
      if(mapper.getVertexTypeByName(columnName) == null) {
        candidateName = columnName;
      }
      else {
        candidateName = this.toJavaClassConvention(relationship.getForeignEntityName()) + "2" + this.toJavaClassConvention(relationship.getParentEntityName());
      }
    }

    // Foreign Key composed of multiple attribute
    else {         
      candidateName = this.toJavaClassConvention(relationship.getForeignEntityName()) + "2" + this.toJavaClassConvention(relationship.getParentEntityName());
    }

    // check duplicate name and map updating
    if(!mapper.getEdgeTypeName2count().keySet().contains(candidateName)) {
      mapper.getEdgeTypeName2count().put(candidateName, 1);
      finalName = candidateName;
    }
    else {
      int nameOccurences = mapper.getEdgeTypeName2count().get(candidateName);
      if(candidateName.contains("_"))
        finalName = candidateName.substring(0, candidateName.lastIndexOf("_")+1) + (nameOccurences+1);
      else
        finalName = candidateName + "_" + (nameOccurences+1);

      mapper.getEdgeTypeName2count().put(candidateName, nameOccurences+1);
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

  /* (non-Javadoc)
   * @see com.orientechnologies.orient.importer.nameresolver.ONameResolver#reverseStransformation(java.lang.String)
   */
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
