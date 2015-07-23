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

package com.orientechnologies.orient.teleport.context;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.teleport.ui.OStatisticsListener;

/**
 * Collects and updates statistics about the Drakkar execution state.
 * It identifies and monitors 4 step in the global execution:
 * 1. Source DB Schema building
 * 2. Graph Model building
 * 3. OrientDB Schema writing
 * 4. OrientDB importing
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OTeleportStatistics {

  // indicates the running step, -1 if no step are running
  public volatile int runningStepNumber;

  // Source DB Schema building statistics
  public volatile int totalNumberOfEntities;
  public volatile int builtEntities;
  public volatile int doneEntity4Relationship;
  public volatile int detectedRelationships;
  public volatile Date startWork1Time;

  // Graph Model building statistics
  public volatile int totalNumberOfModelVertices;
  public volatile int builtModelVertexTypes;
  public volatile int totalNumberOfRelationships;
  public volatile int analizedRelationships;
  public volatile int builtModelEdgeTypes;
  public volatile Date startWork2Time;

  // OrientDB Schema writing statistics
  public volatile int totalNumberOfVertexType;
  public volatile int wroteVertexType;
  public volatile int totalNumberOfEdgeType;
  public volatile int wroteEdgeType;
  public volatile int totalNumberOfIndices;
  public volatile int wroteIndices;
  public volatile Date startWork3Time;

  // OrientDB importing
  public volatile int totalNumberOfRecords;
  public volatile int importedRecords;
  public volatile int orientVertices;
  public volatile int orientEdges;
  public volatile Date startWork4Time;

  // Warnings Messages
  public volatile Set<String> warningMessages;

  // Listeners
  private volatile List<OStatisticsListener> listeners;

  public OTeleportStatistics() {
    this.init();
    this.warningMessages = new HashSet<String>();
    this.listeners = new ArrayList<OStatisticsListener>();
  }


  private void init() {

    this.runningStepNumber = -1;

    this.totalNumberOfEntities = 0;
    this.builtEntities = 0;
    this.doneEntity4Relationship = 0;
    this.detectedRelationships = 0;

    this.totalNumberOfModelVertices = 0;
    this.builtModelVertexTypes = 0;
    this.totalNumberOfRelationships = 0;
    this.analizedRelationships = 0;
    this.builtModelEdgeTypes = 0;

    this.totalNumberOfVertexType = 0;
    this.wroteVertexType = 0;
    this.totalNumberOfEdgeType = 0;
    this.wroteEdgeType = 0;
    this.totalNumberOfIndices = 0;
    this.wroteIndices = 0;

    this.totalNumberOfRecords = 0;
    this.importedRecords = 0;
    this.orientVertices = 0;
    this.orientEdges = 0;

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
    s += "\nRelationships: " + this.detectedRelationships;
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
    s += "\nIndices: " + this.wroteIndices;
    return s;
  }

  public String importingProgress() {
    String s ="OrientDB Importing\n";
    s += "Imported Records: " + this.importedRecords + "/" + this.totalNumberOfRecords;
    s += "\nVertices on OrientDB: " + this.orientVertices;
    s += "\nEdges on OrientDB: " + this.orientEdges;

    return s;
  }

  public String toString() {
    String s = "\n\nSUMMARY\n\n";
    s += this.sourceDbSchemaBuildingProgress() + "\n\n" + this.orientSchemaWritingProgress() + "\n\n" + this.importingProgress() + "\n\n";

    if(this.warningMessages.size() > 0) {
      s += "Warning Messages:\n";
      for(String message: this.warningMessages) {
        s += message + "\n";
      }
    }
    return s;
  }



}
