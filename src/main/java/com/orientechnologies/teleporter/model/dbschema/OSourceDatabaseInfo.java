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

package com.orientechnologies.teleporter.model.dbschema;

import com.orientechnologies.teleporter.model.OSourceInfo;
import java.util.List;

/** Represents a source database with all its related info for accessing it. */
public class OSourceDatabaseInfo implements OSourceInfo {

  private String sourceIdName;
  private String driverName;
  private String url;
  private String username;
  private String password;
  private List<String> primaryKey;

  public OSourceDatabaseInfo(
      String sourceIdName, String driverName, String url, String username, String password) {
    this(sourceIdName, driverName, url, username, password, null);
  }

  public OSourceDatabaseInfo(
      String sourceIdName,
      String driverName,
      String url,
      String username,
      String password,
      List<String> primaryKey) {
    this.sourceIdName = sourceIdName;
    this.driverName = driverName;
    this.url = url;
    this.username = username;
    this.password = password;
    this.primaryKey = primaryKey;
  }

  public String getSourceIdName() {
    return this.sourceIdName;
  }

  public void setSourceIdName(String sourceIdName) {
    this.sourceIdName = sourceIdName;
  }

  public String getDriverName() {
    return this.driverName;
  }

  public void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  public String getUrl() {
    return this.url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUsername() {
    return this.username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return this.password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setPrimaryKey(List<String> primaryKey) {
    this.primaryKey = primaryKey;
  }

  @Override
  public int hashCode() {
    int result = sourceIdName.hashCode();
    result = 31 * result + driverName.hashCode();
    result = 31 * result + url.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OSourceDatabaseInfo that = (OSourceDatabaseInfo) o;

    if (!sourceIdName.equals(that.sourceIdName)) return false;
    if (!driverName.equals(that.driverName)) return false;
    return url.equals(that.url);
  }

  public List<String> getPrimaryKey() {
    return primaryKey;
  }
}
