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

/**
 * Interface that performs name transformations on the elements
 * of the data source according to a specific convention.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public interface ONameResolver {

  public String resolveVertexName(String candidateName);

  public String resolveVertexProperty(String candidateName);

  public String resolveEdgeName(OCanonicalRelationship relationship);

}