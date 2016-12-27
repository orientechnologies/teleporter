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

import com.orientechnologies.teleporter.model.dbschema.ORelationship;

/**
 * Implementation of ONameResolver that maintains the original name convention.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */

public class OOriginalConventionNameResolver implements ONameResolver {

  @Override
  public String resolveVertexName(String candidateName) {

    candidateName = candidateName.replace(" ", "_");
    return candidateName;
  }

  @Override
  public String resolveVertexProperty(String candidateName) {

    candidateName = candidateName.replace(" ", "_");
    return candidateName;
  }

  @Override
  public String resolveEdgeName(ORelationship relationship) {

    String finalName;

    // Foreign Key composed of 1 attribute
    if (relationship.getForeignKey().getInvolvedAttributes().size() == 1) {
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
      finalName = relationship.getForeignEntity() + "2" + relationship.getParentEntity();
    }

    return finalName;
  }

}
