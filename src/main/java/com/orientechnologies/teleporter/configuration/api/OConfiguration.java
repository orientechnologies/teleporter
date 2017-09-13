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

package com.orientechnologies.teleporter.configuration.api;

import com.orientechnologies.teleporter.model.dbschema.OEntity;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * It collects all the information contained in the migrationConfigDoc submitted by the user.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OConfiguration {

  private static OConfiguration instance = null;

  private List<OConfiguredVertexClass> configuredVertices;  // may be empty but not null
  private List<OConfiguredEdgeClass>   configuredEdges;       // may be empty but not null

  public OConfiguration() {
    this.configuredVertices = new LinkedList<OConfiguredVertexClass>();
    this.configuredEdges = new LinkedList<OConfiguredEdgeClass>();
  }

  public static OConfiguration getInstance() {
    if (instance == null) {
      instance = new OConfiguration();
    }
    return instance;
  }

  public List<OConfiguredVertexClass> getConfiguredVertices() {
    return this.configuredVertices;
  }

  public void setConfiguredVertices(List<OConfiguredVertexClass> configuredVertices) {
    this.configuredVertices = configuredVertices;
  }

  public List<OConfiguredEdgeClass> getConfiguredEdges() {
    return this.configuredEdges;
  }

  public void setConfiguredEdges(List<OConfiguredEdgeClass> configuredEdges) {
    this.configuredEdges = configuredEdges;
  }

  public OConfiguredVertexClass getVertexClassByName(String vertexClassName) {

    for (OConfiguredVertexClass currVertexClass : this.configuredVertices) {
      if (currVertexClass.getName().equals(vertexClassName)) {
        return currVertexClass;
      }
    }
    return null;
  }

  public OConfiguredEdgeClass getEdgeClassByName(String edgeClassName) {

    for (OConfiguredEdgeClass currEdgeClass : this.configuredEdges) {
      if (currEdgeClass.getName().equals(edgeClassName)) {
        return currEdgeClass;
      }
    }
    return null;
  }

  public OConfiguredVertexClass getVertexByMappedEntities(List<OEntity> mappedEntities) {

    for (OConfiguredVertexClass currConfiguredVertex : this.configuredVertices) {
      boolean isTargetVertex = this.isTargetVertex(currConfiguredVertex, mappedEntities);

      if (isTargetVertex) {
        return currConfiguredVertex;
      }
    }
    return null;
  }

  private boolean isTargetVertex(OConfiguredVertexClass currConfiguredVertex, List<OEntity> mappedEntities) {

    List<OSourceTable> sourceTables = currConfiguredVertex.getMapping().getSourceTables();

    for (OEntity currEntity : mappedEntities) {
      String entityName = currEntity.getName();

      boolean containsEntity = false;
      for (OSourceTable sourceTable : sourceTables) {
        if (sourceTable.getTableName().equals(entityName)) {
          containsEntity = true;
          break;
        }
      }
      if (!containsEntity) {
        return false;
      }
    }

    return true;
  }

  public OConfiguredVertexClass getVertexClassByTableName(String tableName) {

    for (OConfiguredVertexClass vertexClass : this.configuredVertices) {
      List<OSourceTable> sourceTables = vertexClass.getMapping().getSourceTables();
      for (OSourceTable sourceTable : sourceTables) {
        if (sourceTable.getTableName().equals(tableName)) {
          return vertexClass;
        }
      }
    }
    return null;
  }

  public Map<String, List<OConfiguredVertexClass>> buildTableName2MappedConfiguredVertices() {

    Map<String, List<OConfiguredVertexClass>> tableName2mappedConfiguredVertices = new HashMap<String, List<OConfiguredVertexClass>>();

    for (OConfiguredVertexClass currConfiguredVertexClass : this.configuredVertices) {
      for (OSourceTable currSourceTable : currConfiguredVertexClass.getMapping().getSourceTables()) {
        String tableName = currSourceTable.getTableName();
        List<OConfiguredVertexClass> mappedVertices = tableName2mappedConfiguredVertices.get(tableName);
        if (mappedVertices == null) {
          mappedVertices = new LinkedList<OConfiguredVertexClass>();
        }
        mappedVertices.add(currConfiguredVertexClass);
        tableName2mappedConfiguredVertices.put(tableName, mappedVertices);
      }
    }
    return tableName2mappedConfiguredVertices;
  }
}
