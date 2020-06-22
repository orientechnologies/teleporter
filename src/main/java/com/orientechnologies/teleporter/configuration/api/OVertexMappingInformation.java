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
public class OVertexMappingInformation {

  private OConfiguredVertexClass belongingVertex; // mandatory
  private List<OSourceTable> sourceTables; // must be not empty!
  private String aggregationFunction; // optional (not present when there is not any aggregation)

  public OVertexMappingInformation(OConfiguredVertexClass belongingVertex) {
    this.belongingVertex = belongingVertex;
  }

  public OConfiguredVertexClass getBelongingVertex() {
    return this.belongingVertex;
  }

  public void setBelongingVertex(OConfiguredVertexClass belongingVertex) {
    this.belongingVertex = belongingVertex;
  }

  public List<OSourceTable> getSourceTables() {
    return this.sourceTables;
  }

  public void setSourceTables(List<OSourceTable> sourceTables) {
    this.sourceTables = sourceTables;
  }

  public String getAggregationFunction() {
    return this.aggregationFunction;
  }

  public void setAggregationFunction(String aggregationFunction) {
    this.aggregationFunction = aggregationFunction;
  }
}
