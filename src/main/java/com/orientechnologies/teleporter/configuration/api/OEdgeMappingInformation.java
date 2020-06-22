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

import java.util.List;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OEdgeMappingInformation {

  private OConfiguredEdgeClass belongingEdge; // mandatory
  private String fromTableName; // mandatory
  private String toTableName; // mandatory
  private List<String> fromColumns; // mandatory
  private List<String> toColumns; // mandatory
  private String direction; // mandatory
  private OAggregatedJoinTableMapping
      representedJoinTableMapping; // may be null if the edge does not represent a join table

  // Lazy loading: classes are stored in this variables in order to avoid a double scan of the
  // vertices when fromProperty and toProperty are requested.
  private OConfiguredVertexClass fromVertexClass;
  private OConfiguredVertexClass toVertexClass;

  public OEdgeMappingInformation(OConfiguredEdgeClass belongingEdge) {
    this.belongingEdge = belongingEdge;
  }

  public OConfiguredEdgeClass getBelongingEdge() {
    return this.belongingEdge;
  }

  public void setBelongingEdge(OConfiguredEdgeClass belongingEdge) {
    this.belongingEdge = belongingEdge;
  }

  public String getFromTableName() {
    return this.fromTableName;
  }

  public void setFromTableName(String fromTableName) {
    this.fromTableName = fromTableName;
  }

  public String getToTableName() {
    return this.toTableName;
  }

  public void setToTableName(String toTableName) {
    this.toTableName = toTableName;
  }

  public List<String> getFromColumns() {
    return this.fromColumns;
  }

  public void setFromColumns(List<String> fromColumns) {
    this.fromColumns = fromColumns;
  }

  public List<String> getToColumns() {
    return this.toColumns;
  }

  public void setToColumns(List<String> toColumns) {
    this.toColumns = toColumns;
  }

  public String getFromClass() {
    OConfiguredVertexClass fromVertexClass = null;
    if (this.direction.equals("direct")) {
      if (this.fromVertexClass == null) {
        this.fromVertexClass =
            belongingEdge.getGlobalConfiguration().getVertexClassByTableName(this.fromTableName);
      }
      fromVertexClass = this.fromVertexClass;
    } else if (this.direction.equals("inverse")) {
      if (this.toVertexClass == null) {
        this.toVertexClass =
            belongingEdge.getGlobalConfiguration().getVertexClassByTableName(this.toTableName);
      }
      fromVertexClass = this.toVertexClass;
    }
    return fromVertexClass.getName();
  }

  public String getToClass() {
    OConfiguredVertexClass toVertexClass = null;
    if (this.direction.equals("direct")) {
      if (this.toVertexClass == null) {
        this.toVertexClass =
            belongingEdge.getGlobalConfiguration().getVertexClassByTableName(this.toTableName);
      }
      toVertexClass = this.toVertexClass;
    } else if (this.direction.equals("inverse")) {
      if (this.fromVertexClass == null) {
        this.fromVertexClass =
            belongingEdge.getGlobalConfiguration().getVertexClassByTableName(this.fromTableName);
      }
      toVertexClass = this.fromVertexClass;
    }
    return toVertexClass.getName();
  }

  public String[] getFromProperties() {
    if (this.direction.equals("direct")) {
      if (this.fromVertexClass == null) {
        this.fromVertexClass =
            belongingEdge.getGlobalConfiguration().getVertexClassByTableName(this.fromTableName);
      }
      return this.fromVertexClass.getPropertiesByColumns(this.fromColumns);
    } else if (this.direction.equals("inverse")) {
      if (this.toVertexClass == null) {
        this.toVertexClass =
            belongingEdge.getGlobalConfiguration().getVertexClassByTableName(this.toTableName);
      }
      return this.toVertexClass.getPropertiesByColumns(this.toColumns);
    }
    return null;
  }

  public String[] getToProperties() {
    if (this.direction.equals("direct")) {
      if (this.toVertexClass == null) {
        this.toVertexClass =
            belongingEdge.getGlobalConfiguration().getVertexClassByTableName(this.toTableName);
      }
      return this.toVertexClass.getPropertiesByColumns(this.toColumns);
    } else if (this.direction.equals("inverse")) {
      if (this.fromVertexClass == null) {
        this.fromVertexClass =
            belongingEdge.getGlobalConfiguration().getVertexClassByTableName(this.fromTableName);
      }
      return this.fromVertexClass.getPropertiesByColumns(this.fromColumns);
    }
    return null;
  }

  public String getDirection() {
    return this.direction;
  }

  public void setDirection(String direction) {
    this.direction = direction;
  }

  public OAggregatedJoinTableMapping getRepresentedJoinTableMapping() {
    return this.representedJoinTableMapping;
  }

  public void setRepresentedJoinTableMapping(
      OAggregatedJoinTableMapping representedJoinTableMapping) {
    this.representedJoinTableMapping = representedJoinTableMapping;
  }
}
