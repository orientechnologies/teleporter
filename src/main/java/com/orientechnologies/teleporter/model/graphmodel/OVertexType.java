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

import com.orientechnologies.orient.core.record.ODirection;

import java.util.*;

/**
 * It represents an Orient class of a specific type that extends the Orient Vertex Class.
 * It's a simple vertex-type in the graph model.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OVertexType extends OElementType {

  private List<OEdgeType> inEdgesType;
  private List<OEdgeType> outEdgesType;
  private boolean         isFromJoinTable;
  private Set<String>     externalKey;
  private boolean         analyzedInLastMigration;

  public OVertexType(String vertexType) {
    super(vertexType);
    this.inEdgesType = new ArrayList<OEdgeType>();
    this.outEdgesType = new ArrayList<OEdgeType>();
    this.externalKey = new LinkedHashSet<String>();
    this.analyzedInLastMigration = false;
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

  public void setFromJoinTable(boolean fromJoinTable) {
    isFromJoinTable = fromJoinTable;
  }

  public Set<String> getExternalKey() {
    return this.externalKey;
  }

  public void setExternalKey(Set<String> externalKey) {
    this.externalKey = externalKey;
  }

  public boolean isAnalyzedInLastMigration() {
    return this.analyzedInLastMigration;
  }

  public void setAnalyzedInLastMigration(boolean analyzedInLastMigration) {
    this.analyzedInLastMigration = analyzedInLastMigration;
  }

  public OEdgeType getEdgeByName(String edgeName) {

    for (OEdgeType currentEdgeType : this.inEdgesType) {
      if (currentEdgeType.getName().equals(edgeName))
        return currentEdgeType;
    }

    for (OEdgeType currentEdgeType : this.outEdgesType) {
      if (currentEdgeType.getName().equals(edgeName))
        return currentEdgeType;
    }

    return null;

  }

  public OEdgeType getEdgeByName(String name, ODirection direction) {

    if (direction.equals(ODirection.IN)) {
      for (OEdgeType currentEdgeType : this.inEdgesType) {
        if (currentEdgeType.getName().equals(name))
          return currentEdgeType;
      }
    } else if (direction.equals(ODirection.OUT)) {
      for (OEdgeType currentEdgeType : this.outEdgesType) {
        if (currentEdgeType.getName().equals(name))
          return currentEdgeType;
      }
    } else if (direction.equals(ODirection.BOTH)) {
      return this.getEdgeByName(name);
    }

    return null;
  }

  public boolean isFromJoinTable() {
    return this.isFromJoinTable;
  }

  public void setIsFromJoinTable(boolean isFromJoinTable) {
    this.isFromJoinTable = isFromJoinTable;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((super.name == null) ? 0 : super.name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {

    OVertexType that = (OVertexType) obj;

    // check on type and many-to-many variables
    if (!(super.name.equals(that.getName()) && this.isFromJoinTable == that.isFromJoinTable()))
      return false;

    // check on properties
    if (!(this.properties.equals(that.getProperties())))
      return false;

    // in&out edges
    if (!(this.inEdgesType.equals(that.getInEdgesType()) && this.outEdgesType.equals(that.getOutEdgesType())))
      return false;

    return true;
  }

  public String toString() {
    String s =
        "Vertex-type [type = " + super.name + ", # attributes = " + this.properties.size() + ", # inEdges: " + this.inEdgesType
            .size() + ", # outEdges: " + this.outEdgesType.size() + "]\nAttributes:\n";

    for (OModelProperty currentProperty : this.properties) {
      s += currentProperty.getOrdinalPosition() + ": " + currentProperty.getName() + " --> " + currentProperty.toString();

      if (currentProperty.isFromPrimaryKey())
        s += "(from PK)";

      s += "\t";
    }
    return s;
  }

}
