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

package com.orientechnologies.orient.importer.model.graphmodel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 *
 */

public class OVertexType {

  private String vertexType;
  private Map<String,OAttributeProperties> attributeName2attributeProperties;
  private OVertexType parentVertexType;
  private List<OEdgeType> inEdgesType;
  private List<OEdgeType> outEdgesType;
  private boolean fromJunctionEntity;

  public OVertexType(String vertexType) {
    this.vertexType = vertexType;
    this.attributeName2attributeProperties = new LinkedHashMap<String, OAttributeProperties>();
    this.inEdgesType = new ArrayList<OEdgeType>();
    this.outEdgesType = new ArrayList<OEdgeType>();
  }

  public String getType() {
    return this.vertexType;
  }

  public void setType(String vertexType) {
    this.vertexType = vertexType;
  }

  public Map<String, OAttributeProperties> getAttributeName2attributeProperties() {
    return this.attributeName2attributeProperties;
  }

  public void setAttributeName2attributeProperties(Map<String, OAttributeProperties> attributeName2attributeType) {
    this.attributeName2attributeProperties = attributeName2attributeType;
  }

  public OVertexType getParentVertexType() {
    return this.parentVertexType;
  }

  public void setParentVertexType(OVertexType parentVertexType) {
    this.parentVertexType = parentVertexType;
  }

  public List<OEdgeType> getInEdgesType() {
    return this.inEdgesType;
  }

  public void setInEdgesType(List<OEdgeType> inEdgesType) {
    this.inEdgesType = inEdgesType;
  }

  public List<OEdgeType> getOutEdgesType() {
    return this.outEdgesType;
  }

  public void setOutEdgesType(List<OEdgeType> outEdgesType) {
    this.outEdgesType = outEdgesType;
  }

  public boolean isFromJunctionEntity() {
    return this.fromJunctionEntity;
  }

  public void setFromMany2Many(boolean fromMany2Many) {
    this.fromJunctionEntity = fromMany2Many;
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (fromJunctionEntity ? 1231 : 1237);
    result = prime * result + ((parentVertexType == null) ? 0 : parentVertexType.hashCode());
    result = prime * result + ((vertexType == null) ? 0 : vertexType.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {

    OVertexType that = (OVertexType) obj;

    // check on type and many-to-many variables
    if(!(this.vertexType.equals(that.getType()) && this.fromJunctionEntity == that.isFromJunctionEntity()))
      return false;

    // check on properties
    for(String attributeName: this.attributeName2attributeProperties.keySet()) {
      if(!(that.getAttributeName2attributeProperties().containsKey(attributeName) && 
          this.attributeName2attributeProperties.get(attributeName).equals(that.getAttributeName2attributeProperties().get(attributeName))))
        return false;
    }

    return true;
  }

  public String toString() {
    String s = "Vertex-type [type = " + this.vertexType + ", # attributes = " + this.attributeName2attributeProperties.size() + ", # inEdges: "
        + this.inEdgesType.size() + ", # outEdges: " + this.outEdgesType.size() + "]\nAttributes:\n"; 

    for(String attributeName: this.attributeName2attributeProperties.keySet()) {
      s += this.attributeName2attributeProperties.get(attributeName).getOrdinalPosition() + ": " + attributeName + " --> " + this.attributeName2attributeProperties.get(attributeName).toString();
      
      if(this.attributeName2attributeProperties.get(attributeName).isFromPrimaryKey())
        s += "(from PK)";
      
      s += "\t";
    }
    return s;    
  }

}
