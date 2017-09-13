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

import java.util.LinkedList;
import java.util.List;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OConfiguredVertexClass extends OConfiguredClass {

  private List<String>              externalKeyProps;
  private OVertexMappingInformation mapping;                  // mandatory
  private Double x;
  private Double y;
  private Double px;
  private Double py;
  private Integer fixed;

  // boolean value used to specify if the configured vertex was already analyzed and applied to the graph model
  private boolean alreadyAnalyzed;

  public OConfiguredVertexClass(String vertexName, OConfiguration globalConfiguration) {
    super(vertexName, globalConfiguration);
    this.externalKeyProps = new LinkedList<String>();
    this.alreadyAnalyzed = false;
  }

  public OVertexMappingInformation getMapping() {
    return this.mapping;
  }

  public void setMapping(OVertexMappingInformation mapping) {
    this.mapping = mapping;
  }

  public List<String> getExternalKeyProps() {
    return externalKeyProps;
  }

  public void setExternalKeyProps(List<String> externalKeyProps) {
    this.externalKeyProps = externalKeyProps;
  }

  public boolean isAlreadyAnalyzed() {
    return alreadyAnalyzed;
  }

  public void setAlreadyAnalyzed(boolean alreadyAnalyzed) {
    this.alreadyAnalyzed = alreadyAnalyzed;
  }

  public Double getX() {
    return this.x;
  }

  public void setX(Double x) {
    this.x = x;
  }

  public Double getY() {
    return this.y;
  }

  public void setY(Double y) {
    this.y = y;
  }

  public Double getPx() {
    return this.px;
  }

  public void setPx(Double px) {
    this.px = px;
  }

  public Double getPy() {
    return this.py;
  }

  public void setPy(Double py) {
    this.py = py;
  }

  public Integer getFixed() {
    return this.fixed;
  }

  public void setFixed(Integer fixed) {
    this.fixed = fixed;
  }
}
