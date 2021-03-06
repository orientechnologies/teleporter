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

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OSplittingEdgeInformation {

  private String fromVertexClass; // mandatory
  private String toVertexClass; // mandatory
  private String sourceTable; // mandatory

  public OSplittingEdgeInformation(
      String fromVertexClass, String toVertexClass, String sourceTable) {
    this.fromVertexClass = fromVertexClass;
    this.toVertexClass = toVertexClass;
    this.sourceTable = sourceTable;
  }

  public String getFromVertexClass() {
    return this.fromVertexClass;
  }

  public void setFromVertexClass(String fromVertexClass) {
    this.fromVertexClass = fromVertexClass;
  }

  public String getToVertexClass() {
    return this.toVertexClass;
  }

  public void setToVertexClass(String toVertexClass) {
    this.toVertexClass = toVertexClass;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public void setSourceTable(String sourceTable) {
    this.sourceTable = sourceTable;
  }
}
