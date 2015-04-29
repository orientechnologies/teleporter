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

package com.orientechnologies.orient.drakkar.factory;

import com.orientechnologies.orient.drakkar.model.dbschema.ORelationship;
import com.orientechnologies.orient.drakkar.nameresolver.ONameResolver;

/**
 * Implementation of ONameResolver that maintains the original name convention.
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class OOriginalConventionNameResolver implements ONameResolver {

  
  @Override
  public String resolveVertexName(String candidateName) {
    return candidateName;
  }

 
  @Override
  public String resolveVertexProperty(String candidateName) {
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
      finalName = "has_" + columnName;
    }

    // Foreign Key composed of multiple attribute
    else {         
      finalName = relationship.getForeignEntityName() + "2" + relationship.getParentEntityName();
    }

    return finalName;
  }

  
  @Override
  public String reverseTransformation(String transformedName) {
    return transformedName;
  }

}
