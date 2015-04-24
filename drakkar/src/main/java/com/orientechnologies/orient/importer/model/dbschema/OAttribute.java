/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.importer.model.dbschema;

/**
 * <CLASS DESCRIPTION>
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 * 
 */

public class OAttribute implements Comparable<OAttribute> {

  private String name;
  private int ordinalPosition;
  private String dataType;
  private OEntity belongingEntity;


  public OAttribute(String name, int ordinalPosition, String dataType, OEntity belongingEntity) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.dataType = dataType;
    this.belongingEntity = belongingEntity;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getOrdinalPosition() {
    return this.ordinalPosition;
  }

  public void setOrdinalPosition(int ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
  }

  public String getDataType() {
    return this.dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public OEntity getBelongingEntity() {
    return this.belongingEntity;
  }

  public void setBelongingEntity(OEntity belongingEntity) {
    this.belongingEntity = belongingEntity;
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ordinalPosition;
    return result;
  }

  public boolean equals(Object o) {
    OAttribute that = (OAttribute) o;
    if(this.belongingEntity.getName().equals(that.getBelongingEntity().getName()) && this.name.equals(that.getName()) 
        && this.dataType.equals(that.getDataType()) && this.ordinalPosition == that.ordinalPosition) {
      return true;
    }
    else
      return false;

  }

 
  @Override
  public int compareTo(OAttribute attributeToCompare) {

    if(this.ordinalPosition > attributeToCompare.getOrdinalPosition())
      return 0;

    else if(this.ordinalPosition < attributeToCompare.getOrdinalPosition())
      return -1;
    
    else
      return 1;
  }





}
