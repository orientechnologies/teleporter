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
public class OConfiguredProperty {

  private String propertyName; // mandatory
  private boolean isIncludedInMigration; // mandatory
  private String propertyType; // mandatory
  private int ordinalPosition; // mandatory
  private boolean isMandatory; // mandatory
  private boolean isReadOnly; // mandatory
  private boolean isNotNull; // mandatory
  private OConfiguredPropertyMapping
      propertyMapping; // may be null if the property is defined from scratch (only schema
  // definition)

  public OConfiguredProperty(String propertyName) {
    this.propertyName = propertyName;
  }

  public String getPropertyName() {
    return this.propertyName;
  }

  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }

  public boolean isIncludedInMigration() {
    return this.isIncludedInMigration;
  }

  public void setIncludedInMigration(boolean includedInMigration) {
    this.isIncludedInMigration = includedInMigration;
  }

  public String getPropertyType() {
    return this.propertyType;
  }

  public void setPropertyType(String propertyType) {
    this.propertyType = propertyType;
  }

  public int getOrdinalPosition() {
    return this.ordinalPosition;
  }

  public void setOrdinalPosition(int ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
  }

  public boolean isMandatory() {
    return this.isMandatory;
  }

  public void setMandatory(boolean mandatory) {
    this.isMandatory = mandatory;
  }

  public boolean isReadOnly() {
    return this.isReadOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.isReadOnly = readOnly;
  }

  public boolean isNotNull() {
    return this.isNotNull;
  }

  public void setNotNull(boolean notNull) {
    this.isNotNull = notNull;
  }

  public OConfiguredPropertyMapping getPropertyMapping() {
    return this.propertyMapping;
  }

  public void setPropertyMapping(OConfiguredPropertyMapping propertyMapping) {
    this.propertyMapping = propertyMapping;
  }
}
