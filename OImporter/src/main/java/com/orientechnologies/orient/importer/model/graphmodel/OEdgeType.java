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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 *
 */

public class OEdgeType {

  private String edgeType;
  private OVertexType inVertexType;
  private OVertexType outVertexType;
  private Map<String,OAttributeProperties> attributeName2attributeProperties;

  public OEdgeType(String edgeType) {
    this.edgeType = edgeType;    
  }

  public OEdgeType(String edgeType, OVertexType outVertexType, OVertexType inVertexType) {
    this.edgeType = edgeType;
    this.outVertexType = outVertexType;
    this.inVertexType = inVertexType;
    this.attributeName2attributeProperties = new LinkedHashMap<String, OAttributeProperties>();

  }

  public String getType() {
    return this.edgeType;
  }

  public void setType(String edgeType) {
    this.edgeType = edgeType;
  }

  public OVertexType getInVertexType() {
    return this.inVertexType;
  }

  public void setInVertexType(OVertexType inVertexType) {
    this.inVertexType = inVertexType;
  }

  public OVertexType getOutVertexType() {
    return outVertexType;
  }

  public void setOutVertexType(OVertexType outVertexType) {
    this.outVertexType = outVertexType;
  }

  public Map<String, OAttributeProperties> getAttributeName2attributeProperties() {
    return this.attributeName2attributeProperties;
  }

  public void setAttributeName2attributeProperties(Map<String, OAttributeProperties> attributeName2attributeProperties) {
    this.attributeName2attributeProperties = attributeName2attributeProperties;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((edgeType == null) ? 0 : edgeType.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {

    OEdgeType that = (OEdgeType) obj;

    // check on type and in/out vertex
    if(!(this.edgeType.equals(that.getType()) && this.outVertexType.equals(that.getOutVertexType()) && this.inVertexType.equals(that.getInVertexType())))
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
    String s = "Edge-type [type = " + this.edgeType + ", out-vertex-type = " + this.getOutVertexType().getType() +  ", in-vertex-type = " + this.getInVertexType().getType() + " ]"; 

    if(this.attributeName2attributeProperties.size() > 0) {
      s += "\nEdge's properties ("+this.attributeName2attributeProperties.size()+"):\n";
      for(String property: this.attributeName2attributeProperties.keySet()) {
        s += property + " --> " + this.attributeName2attributeProperties.get(property).toString() + "\n";
      }
    }
    s += "\n";
    return s; 

  }


}
