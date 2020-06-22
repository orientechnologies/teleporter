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
public class OConfiguredEdgeClass extends OConfiguredClass {

  // mappings and splittingEdgeInfo are mutually exclusive
  private List<OEdgeMappingInformation> mappings; // mandatory
  private OSplittingEdgeInformation splittingEdgeInfo; // mandatory

  private boolean isLogical; // optional

  public OConfiguredEdgeClass(String edgeName, OConfiguration globalConfiguration) {
    super(edgeName, globalConfiguration);
    this.isLogical = false;
  }

  public List<OEdgeMappingInformation> getMappings() {
    return this.mappings;
  }

  public void setMappings(List<OEdgeMappingInformation> mappings) {
    this.mappings = mappings;
  }

  public OSplittingEdgeInformation getSplittingEdgeInfo() {
    return this.splittingEdgeInfo;
  }

  public void setSplittingEdgeInfo(OSplittingEdgeInformation splittingEdgeInfo) {
    this.splittingEdgeInfo = splittingEdgeInfo;
  }

  public boolean isLogical() {
    return this.isLogical;
  }

  public void setLogical(boolean logical) {
    this.isLogical = logical;
  }
}
