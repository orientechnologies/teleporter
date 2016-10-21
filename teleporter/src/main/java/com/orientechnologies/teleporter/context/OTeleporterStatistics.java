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

package com.orientechnologies.teleporter.context;

import com.orientechnologies.teleporter.ui.OStatisticsListener;

import java.util.*;

/**
 * Collects and updates statistics about the Drakkar execution state.
 * It identifies and monitors 4 step in the global execution:
 * 1. Source DB Schema building
 * 2. Graph Model building
 * 3. OrientDB Schema writing
 * 4. OrientDB importing
 * 
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public class OTeleporterStatistics {

  // indicates the running step, -1 if no step are running
  public volatile int runningStepNumber;

  // Source DB Schema building statistics
  public volatile int totalNumberOfEntities;
  public volatile int builtEntities;
  public volatile int entitiesAnalyzedForRelationship;  // used only for te progress monitor because we can't know the total number of relationships before all the entities are scanned.
  public volatile int builtRelationships;
  public volatile int totalNumberOfRelationships;
  public volatile Date startWork1Time;

  // Graph Model building statistics
  public volatile int totalNumberOfModelVertices;
  public volatile int builtModelVertexTypes;
  public volatile int totalNumberOfModelEdges;
  public volatile int builtModelEdgeTypes;
  public volatile Date startWork2Time;

  // OrientDB Schema writing statistics
  public volatile int totalNumberOfVertexTypes;
  public volatile int wroteVertexType;
  public volatile int totalNumberOfEdgeTypes;
  public volatile int wroteEdgeType;
  public volatile int totalNumberOfIndices;
  public volatile int wroteIndexes;
  public volatile Date startWork3Time;

  // OrientDB importing
  public volatile int totalNumberOfRecords;
  public volatile int analyzedRecords;
  public volatile int orientAddedVertices;
  public volatile int orientUpdatedVertices;
  public volatile int orientAddedEdges;
  public volatile Date startWork4Time;

  // Warnings and Error Messages
  public volatile Set<String> warningMessages;
  public volatile Set<String> errorMessages;

  // Listeners
  private volatile List<OStatisticsListener> listeners;

  public OTeleporterStatistics() {
    this.init();
    this.warningMessages = new HashSet<String>();
    this.errorMessages = new HashSet<String>();
    this.listeners = new ArrayList<OStatisticsListener>();
  }


  private void init() {

    this.runningStepNumber = -1;

    this.totalNumberOfEntities = 0;
    this.builtEntities = 0;
    this.entitiesAnalyzedForRelationship = 0;
    this.builtRelationships = 0;
    this.totalNumberOfRelationships = 0;

    this.totalNumberOfModelVertices = 0;
    this.builtModelVertexTypes = 0;
    this.totalNumberOfModelEdges = 0;
    this.builtModelEdgeTypes = 0;

    this.totalNumberOfVertexTypes = 0;
    this.wroteVertexType = 0;
    this.totalNumberOfEdgeTypes = 0;
    this.wroteEdgeType = 0;
    this.totalNumberOfIndices = 0;
    this.wroteIndexes = 0;

    this.totalNumberOfRecords = 0;
    this.analyzedRecords = 0;
    this.orientAddedVertices = 0;
    this.orientAddedEdges = 0;

  }

  public void reset() {
    this.init();
  }

  /*
   * Publisher-Subscribers
   */

  public void registerListener(OStatisticsListener listener) {
    this.listeners.add(listener);
  }

  public void notifyListeners() {
    for(OStatisticsListener listener: this.listeners) {
      listener.updateOnEvent(this);
    }
  }


  /*
   *  toString methods
   */

  public String sourceDbSchemaBuildingProgress() {
    String s ="Source DB Schema\n";
    s += "Entities: " + this.builtEntities;
    s += "\nRelationships: " + this.builtRelationships;
    return s;
  }

  public String graphModelBuildingProgress() {
    String s ="Graph Model Building\n";
    s += "Built Model Vertices: " + this.builtModelVertexTypes;
    s += "\nBuilt Model Edges: " + this.builtModelEdgeTypes;
    return s;
  }

  public String orientSchemaWritingProgress() {
    String s ="OrientDB Schema\n";
    s += "Vertex Type: " + this.wroteVertexType;
    s += "\nEdge Type: " + this.wroteEdgeType;
    s += "\nIndexes: " + this.wroteIndexes;
    return s;
  }

  public String importingProgress() {
    String s ="OrientDB Importing\n";
    s += "Analyzed Records: " + this.analyzedRecords + "/" + this.totalNumberOfRecords;
    s += "\nAdded Vertices on OrientDB: " + this.orientAddedVertices;
    s += "\nUpdated Vertices on OrientDB: " + this.orientUpdatedVertices;
    s += "\nAdded Edges on OrientDB: " + this.orientAddedEdges;

    return s;
  }

  public String toString() {
    String s = "\n\nSUMMARY\n\n";
    s += this.sourceDbSchemaBuildingProgress() + "\n\n" + this.orientSchemaWritingProgress() + "\n\n" + this.importingProgress() + "\n\n";

    // printing error messages
    if(this.errorMessages.size() > 0) {
      s += "Error Messages:\n";
      for(String message: this.errorMessages) {
        s += message + "\n";
      }
    }

    s += "\n\n";

    // printing warning messages
    if(this.warningMessages.size() > 0) {
      s += "Warning Messages:\n";
      for(String message: this.warningMessages) {
        s += message + "\n";
      }
    }
    return s;
  }



}
