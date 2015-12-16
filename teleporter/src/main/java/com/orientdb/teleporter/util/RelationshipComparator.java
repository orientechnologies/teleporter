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

package com.orientdb.teleporter.util;

import java.util.Comparator;

import com.orientdb.teleporter.model.dbschema.ORelationship;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class RelationshipComparator implements Comparator<ORelationship> {

  /* (non-Javadoc)
   * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
   */
  @Override
  public int compare(ORelationship r1, ORelationship r2) {
    if(!r1.getForeignEntityName().equals(r2.getForeignEntityName()))
      return r1.getForeignEntityName().compareTo(r2.getForeignEntityName());
    else
      return r1.getForeignKey().getInvolvedAttributes().get(0).getName().compareTo(r2.getForeignKey().getInvolvedAttributes().get(0).getName());
  }

}
