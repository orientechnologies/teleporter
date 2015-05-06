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

package com.orientechnologies.orient.drakkar.model.graphmodel;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * It represents an Orient class of a specific type that extends the Orient Edge Class.
 * It's a simple edge-type in the graph model.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OEdgeType {

  private String edgeType;
  private OVertexType inVertexType;
  private OVertexType outVertexType;
//  private Map<String,OProperty> attributeName2attributeProperties;
  private List<OProperty> properties;


  public OEdgeType(String edgeType) {
    this.edgeType = edgeType;  
//    this.attributeName2attributeProperties = new LinkedHashMap<String, OProperty>();
    this.properties = new LinkedList<OProperty>();

  }

  public OEdgeType(String edgeType, OVertexType outVertexType, OVertexType inVertexType) {
    this.edgeType = edgeType;
    this.outVertexType = outVertexType;
    this.inVertexType = inVertexType;
//    this.attributeName2attributeProperties = new LinkedHashMap<String, OProperty>();
    this.properties = new LinkedList<OProperty>();


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

//  public Map<String, OProperty> getAttributeName2attributeProperties() {
//    return this.attributeName2attributeProperties;
//  }
//
//  public void setAttributeName2attributeProperties(Map<String, OProperty> attributeName2attributeProperties) {
//    this.attributeName2attributeProperties = attributeName2attributeProperties;
//  }
  
  public List<OProperty> getProperties() {
    return this.properties;
  }

  public void setProperties(List<OProperty> properties) {
    this.properties = properties;
  }
  
  /**
   * @param toRemove
   */
  public void removePropertyByName(String toRemove) {
    Iterator<OProperty> it = this.properties.iterator();
    OProperty currentProperty = null;

    while (it.hasNext()) {
      currentProperty = it.next();
      if(currentProperty.getName().equals(toRemove))
        it.remove();
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((edgeType == null) ? 0 : edgeType.hashCode());
    result = prime * result + ((inVertexType == null) ? 0 : inVertexType.hashCode());
    result = prime * result + ((outVertexType == null) ? 0 : outVertexType.hashCode());
    result = prime * result + ((properties == null) ? 0 : properties.hashCode());
    return result;
  }
 
  @Override
  public boolean equals(Object obj) {

    OEdgeType that = (OEdgeType) obj;

    // check on type and in/out vertex
    if(!(this.edgeType.equals(that.getType()) && this.outVertexType.getType().equals(that.getOutVertexType().getType()) && this.inVertexType.getType().equals(that.getInVertexType().getType())))
      return false;

    // check on properties
    for(OProperty currentProperty: this.properties) {
      if(!(that.getProperties().contains(currentProperty)))
        return false;
    }

    return true;

  }


  public String toString() {

    String s = "";

    if(this.outVertexType != null && this.inVertexType != null)
      s = "Edge-type [type = " + this.edgeType + ", out-vertex-type = " + this.getOutVertexType().getType() +  ", in-vertex-type = " + this.getInVertexType().getType() + " ]"; 

    else
      s = "Edge-type [type = " + this.edgeType + " ]"; 

    if(this.properties.size() > 0) {
      s += "\nEdge's properties ("+this.properties.size()+"):\n";
      for(OProperty property: this.properties) {
        s += property.getName() + " --> " + property.toString() + "\n";
      }
    }
    s += "\n";
    return s; 

  }


}
