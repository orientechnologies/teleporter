/*
 *
 *  *  Copyright 2017 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */

package com.orientechnologies.teleporter.configuration.api;

import java.util.List;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OAggregatedJoinTableMapping {

  private String       tableName;           // mandatory
  private List<String> fromColumns;   // mandatory
  private List<String> toColumns;     // mandatory

  public OAggregatedJoinTableMapping(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
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
}
