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

package com.orientechnologies.teleporter.model.graphmodel;

/**
 * It represents an Orient class of a specific type that extends the Orient Edge Class. It's a
 * simple edge-type in the graph model.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OEdgeType extends OElementType {

  private OVertexType inVertexType;
  private OVertexType outVertexType;
  private int numberRelationshipsRepresented; // the number of relationships represented by the edge
  private boolean isSplittingEdge;

  public OEdgeType(String edgeType) {
    super(edgeType);
    this.numberRelationshipsRepresented = 1;
    this.isSplittingEdge = false;
  }

  public OEdgeType(String edgeType, OVertexType outVertexType, OVertexType inVertexType) {
    super(edgeType);
    this.outVertexType = outVertexType;
    this.inVertexType = inVertexType;
    numberRelationshipsRepresented = 1;
    this.isSplittingEdge = false;
  }

  public OEdgeType(
      String edgeType,
      OVertexType outVertexType,
      OVertexType inVertexType,
      int numberRelationshipsRepresented,
      boolean isSplittingEdge) {
    super(edgeType);
    this.outVertexType = outVertexType;
    this.inVertexType = inVertexType;
    this.numberRelationshipsRepresented = numberRelationshipsRepresented;
    this.isSplittingEdge = isSplittingEdge;
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

  public int getNumberRelationshipsRepresented() {
    return this.numberRelationshipsRepresented;
  }

  public void setNumberRelationshipsRepresented(int numberRelationshipsRepresented) {
    this.numberRelationshipsRepresented = numberRelationshipsRepresented;
  }

  public boolean isSplittingEdge() {
    return this.isSplittingEdge;
  }

  public void setSplittingEdge(boolean splittingEdge) {
    isSplittingEdge = splittingEdge;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((super.name == null) ? 0 : super.name.hashCode());
    result = prime * result + ((inVertexType == null) ? 0 : inVertexType.hashCode());
    result = prime * result + ((outVertexType == null) ? 0 : outVertexType.hashCode());
    result = prime * result + ((properties == null) ? 0 : properties.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {

    OEdgeType that = (OEdgeType) obj;

    // check on type and in/out vertex
    if (!(super.name.equals(that.getName())
        && this.inVertexType.getName().equals(that.getInVertexType().getName()))) return false;

    // check on properties
    for (OModelProperty currentProperty : this.properties) {
      if (!(that.getProperties().contains(currentProperty))) return false;
    }

    return true;
  }

  public String toString() {

    String s = "";

    if (this.outVertexType != null && this.inVertexType != null)
      s =
          "Edge-type [type = "
              + super.name
              + ", out-vertex-type = "
              + this.getOutVertexType().getName()
              + ", in-vertex-type = "
              + this.getInVertexType().getName()
              + " ]";
    else s = "Edge-type [type = " + super.name + " ]";

    if (this.properties.size() > 0) {
      s += "\nEdge's properties (" + this.properties.size() + "):\n";
      for (OModelProperty property : this.properties) {
        s += property.getName() + " --> " + property.toString() + "\n";
      }
    }
    s += "\n";
    return s;
  }
}
