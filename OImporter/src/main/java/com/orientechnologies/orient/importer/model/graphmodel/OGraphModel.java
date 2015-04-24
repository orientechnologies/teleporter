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
import java.util.List;


/**
 * <CLASS DESCRIPTION>
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 * 
 */

public class OGraphModel {
  
  private List<OVertexType> verticesType;
  private List<OEdgeType> edgesType;
  
  public OGraphModel() {    
    this.verticesType = new ArrayList<OVertexType>();
    this.edgesType = new ArrayList<OEdgeType>();
  }
  
  public OVertexType getVertexByType(String type) {
    OVertexType vertex = null;
    
    for(OVertexType currentVertex: this.verticesType) {
      if(currentVertex.getType().equalsIgnoreCase(type)) {
        vertex = currentVertex;
        break;
      }
    }    
    return vertex;
  }

  public List<OVertexType> getVerticesType() {
    return this.verticesType;
  }

  public void setVerticesType(List<OVertexType> verticesType) {
    this.verticesType = verticesType;
  }

  public List<OEdgeType> getEdgesType() {
    return this.edgesType;
  }

  public void setEdgesType(List<OEdgeType> edgesType) {
    this.edgesType = edgesType;
  }
  
  public boolean addEdgeType(OEdgeType edgeType) {
    return this.edgesType.add(edgeType);
  }
  
   
 
  public String toString() {
    String s = "\n\n\n------------------------------ MODEL GRAPH DESCRIPTION ------------------------------\n\n\n";
    
    s += "Number of Vertex-type: " + this.verticesType.size() + ".\nNumber of Edge-type: " + this.edgesType.size() + ".\n\n";
    
    // info about vertices
    s += "Vertex-type:\n\n";
    for(OVertexType v: this.verticesType)
      s += v.toString() + "\n\n";
    
    s += "\n\n";
    
    // info about edges
    s += "Edge-type:\n\n";
    for(OEdgeType e: this.edgesType)
      s += e.toString() + "\n";
    
    s += "\n\n";
    
    // graph structure
    s += "Graph structure:\n\n";
    for(OEdgeType e: this.edgesType) {
        s += e.getOutVertexType().getType() + " -----------[" + e.getType() + "]-----------> " + e.getInVertexType().getType() + "\n";
      }
            
    return s; 
  }
  
  
  

}
