/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * OrientDB LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * OrientDB LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from OrientDB LTD.
 * 
 * For more information: http://www.orientdb.com
 */

package com.orientechnologies.teleporter.model.graphmodel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * It represents the model of the destination GraphDB.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OGraphModel {

  private List<OVertexType> verticesType;
  private List<OEdgeType>   edgesType;

  public OGraphModel() {
    this.verticesType = new ArrayList<OVertexType>();
    this.edgesType = new ArrayList<OEdgeType>();
  }

  public OVertexType getVertexTypeByName(String name) {
    OVertexType vertex = null;

    for (OVertexType currentVertex : this.verticesType) {
      if (currentVertex.getName().equals(name)) {
        vertex = currentVertex;
        break;
      }
    }
    return vertex;
  }

  public OVertexType getVertexTypeByNameIgnoreCase(String name) {
    OVertexType vertex = null;

    for (OVertexType currentVertex : this.verticesType) {
      if (currentVertex.getName().equalsIgnoreCase(name)) {
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

  public OEdgeType getEdgeTypeByName(String name) {
    for (OEdgeType currentEdgetype : this.edgesType) {
      if (currentEdgetype.getName().equals(name)) {
        return currentEdgetype;
      }
    }
    return null;
  }

  public OEdgeType getEdgeTypeByNameIgnoreCase(String name) {
    for (OEdgeType currentEdgetype : this.edgesType) {
      if (currentEdgetype.getName().equalsIgnoreCase(name)) {
        return currentEdgetype;
      }
    }
    return null;
  }

  public boolean removeVertexTypeByName(String vertexName) {

    Iterator<OVertexType> iterator = this.verticesType.iterator();

    while (iterator.hasNext()) {
      OVertexType currVertexType = iterator.next();
      if (currVertexType.getName().equals(vertexName)) {

        // removing references from the in edges
        for (OEdgeType currInEdgeType : currVertexType.getInEdgesType()) {
          currInEdgeType.setInVertexType(null);
        }

        // removing the vertex
        iterator.remove();
        return true;
      }
    }
    return false;
  }

  public boolean removeEdgeTypeByName(String vertexName) {

    Iterator<OEdgeType> iterator = this.edgesType.iterator();

    while (iterator.hasNext()) {
      OEdgeType currEdgeType = iterator.next();
      if (currEdgeType.getName().equals(vertexName)) {
        iterator.remove();
        return true;
      }
    }
    return false;
  }

  public String toString() {
    String s = "\n\n\n------------------------------ MODEL GRAPH DESCRIPTION ------------------------------\n\n\n";

    s += "Number of Vertex-type: " + this.verticesType.size() + ".\nNumber of Edge-type: " + this.edgesType.size() + ".\n\n";

    // info about vertices
    s += "Vertex-type:\n\n";
    for (OVertexType v : this.verticesType)
      s += v.toString() + "\n\n";

    s += "\n\n";

    // info about edges
    s += "Edge-type:\n\n";
    for (OEdgeType e : this.edgesType)
      s += e.toString() + "\n";

    s += "\n\n";

    // graph structure
    s += "Graph structure:\n\n";
    for (OVertexType v : this.verticesType) {
      for (OEdgeType e : v.getOutEdgesType())
        s += v.getName() + " -----------[" + e.getName() + "]-----------> " + e.getInVertexType().getName() + "\n";
    }

    return s;
  }

}