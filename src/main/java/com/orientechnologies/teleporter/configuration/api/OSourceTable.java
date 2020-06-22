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
public class OSourceTable {

  private String sourceIdName; // mandatory
  private String dataSource; // mandatory
  private String tableName; // mandatory
  private List<String>
      aggregationColumns; // optional (not present when there is not any aggregation)
  private List<String> primaryKeyColumns;
  private OVertexMappingInformation belongingMapping;

  public OSourceTable(String sourceIdName, OVertexMappingInformation belongingMapping) {
    this.sourceIdName = sourceIdName;
    this.belongingMapping = belongingMapping;
  }

  public String getSourceIdName() {
    return this.sourceIdName;
  }

  public void setSourceIdName(String sourceIdName) {
    this.sourceIdName = sourceIdName;
  }

  public String getDataSource() {
    return this.dataSource;
  }

  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<String> getAggregationColumns() {
    return this.aggregationColumns;
  }

  public void setAggregationColumns(List<String> aggregationColumns) {
    this.aggregationColumns = aggregationColumns;
  }

  public List<String> getPrimaryKeyColumns() {
    return primaryKeyColumns;
  }

  public void setPrimaryKeyColumns(final List<String> primaryKeyColumns) {
    this.primaryKeyColumns = primaryKeyColumns;
  }

  public OVertexMappingInformation getBelongingMapping() {
    return belongingMapping;
  }

  public void setBelongingMapping(OVertexMappingInformation belongingMapping) {
    this.belongingMapping = belongingMapping;
  }
}
