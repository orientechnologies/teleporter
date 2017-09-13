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

package com.orientechnologies.teleporter.util;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

/**
 * Collects several commands executable on a OrientDb database.
 */

public class OGraphCommands {

  private static String quote =  "\"";

  /**
   * The method performs a lookup on the passed ODatabaseDocument for a OVertex, starting from a record and from a vertex type.
   * It returns the vertex if present, null if not present.
   *
   * @param orientGraph
   * @param keys
   * @param values
   * @param vertexClassName
   *
   * @return
   */
  public static OVertex getVertexByIndexedKey(ODatabaseDocument orientGraph, String[] keys, String[] values, String vertexClassName) {

    OVertex vertex = null;

    final OResultSet vertices = getVertices(orientGraph, vertexClassName, keys, values);

    if (vertices.hasNext()) {
      vertex = vertices.next().getVertex().orElse(null);
    }

    vertices.close();

    return vertex;
  }

  public static OResultSet getVertices(ODatabaseDocument orientGraph, String vertexClassName, String[] keys, String[] values) {

    Object[] params = new Object[values.length];

    for (int i = 0; i < params.length; i++) {
      params[i] = values[i];
    }

    String query = "select * from " + vertexClassName + " where ";
    query += keys[0] + " = ?";

    int i;
    for(i=1; i<keys.length; i++) {
      query += " and " + keys[i] + " = ?";
    }
    return orientGraph.command(query, params);
  }
}
