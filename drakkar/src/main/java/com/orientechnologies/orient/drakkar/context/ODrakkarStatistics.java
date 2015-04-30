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

package com.orientechnologies.orient.drakkar.context;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.orientechnologies.orient.drakkar.ui.OStatisticsListener;

/**
 * Collects and updates statistics about the Drakkar execution state.
 * It identifies and monitors 4 step in the global execution:
 * 1. Source DB Schema building
 * 2. Graph Model building
 * 3. OrientDB Schema writing
 * 4. OrientDB importing
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class ODrakkarStatistics {

  // Source DB Schema building statistics
  private int totalNumberOfEntities;
  private int builtEntities;
  private int doneEntity4Relationship;
  private Date startWork1Time;

  // Graph Model building statistics
  private int totalNumberOfModelVertices;
  private int builtModelVertexTypes;
  private int totalNumberOfModelEdges;
  private int builtModelEdgeTypes;
  private Date startWork2Time;

  // OrientDB Schema writing statistics
  private int totalNumberOfVertexType;
  private int wroteVertexType;
  private int totalNumberOfEdgeType;
  private int wroteEdgeType;
  private int totalNumberOfIndices;
  private int wroteIndices;
  private Date startWork3Time;

  // OrientDB importing
  private int totalNumberOfRecords;
  private int importedRecords;
  private Date startWork4Time;

  
  // Listeners
  private List<OStatisticsListener> listeners;

  public ODrakkarStatistics() {
    this.init();
    this.listeners = new ArrayList<OStatisticsListener>();
  }


  private void init() {

    this.totalNumberOfEntities = 0;
    this.builtEntities = 0;
    this.doneEntity4Relationship = 0;

    this.totalNumberOfModelVertices = 0;
    this.builtModelVertexTypes = 0;
    this.totalNumberOfModelEdges = 0;
    this.builtModelEdgeTypes = 0;

    this.totalNumberOfVertexType = 0;
    this.wroteVertexType = 0;
    this.totalNumberOfEdgeType = 0;
    this.wroteEdgeType = 0;
    this.totalNumberOfIndices = 0;
    this.wroteIndices = 0;

    this.totalNumberOfRecords = 0;
    this.importedRecords = 0;

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
  
  public void notifyListeners(int workNumber) {
    for(OStatisticsListener listener: this.listeners) {
      listener.updateOnEvent(this, workNumber);
    }
  }
  

  /*
   *  Getters and Setters
   */

  public int getTotalNumberOfEntities() {
    return this.totalNumberOfEntities;
  }

  public void setTotalNumberOfEntities(int totalNumberOfEntities) {
    this.totalNumberOfEntities = totalNumberOfEntities;
  }

  public int getBuiltEntities() {
    return this.builtEntities;
  }

  public void setWroteEdgeType(int wroteEdgeType) {
    this.wroteEdgeType = wroteEdgeType;
  }


  public int getDoneEntity4Relationship() {
    return this.doneEntity4Relationship;
  }

  public Date getStartWork1Time() {
    return startWork1Time;
  }


  public void setStartWork1Time(Date startWork1Time) {
    this.startWork1Time = startWork1Time;
  }


  public int getTotalNumberOfModelVertices() {
    return this.totalNumberOfModelVertices;
  }

  public void setTotalNumberOfModelVertices(int totalNumberOfModelVertices) {
    this.totalNumberOfModelVertices = totalNumberOfModelVertices;
  }

  public int getBuiltModelVertexTypes() {
    return this.builtModelVertexTypes;
  }


  public int getTotalNumberOfModelEdges() {
    return this.totalNumberOfModelEdges;
  }

  public void setTotalNumberOfModelEdges(int totalNumberOfModelEdges) {
    this.totalNumberOfModelEdges = totalNumberOfModelEdges;
  }

  public int getBuiltModelEdgeTypes() {
    return this.builtModelEdgeTypes;
  }


  public Date getStartWork2Time() {
    return startWork2Time;
  }


  public void setStartWork2Time(Date startWork2Time) {
    this.startWork2Time = startWork2Time;
  }


  public int getTotalNumberOfVertexType() {
    return this.totalNumberOfVertexType;
  }

  public void setTotalNumberOfVertexType(int totalNumberOfVertexType) {
    this.totalNumberOfVertexType = totalNumberOfVertexType;
  }

  public int getWroteVertexType() {
    return this.wroteVertexType;
  }


  public int getTotalNumberOfEdgeType() {
    return this.totalNumberOfEdgeType;
  }

  public void setTotalNumberOfEdgeType(int totalNumberOfEdgeType) {
    this.totalNumberOfEdgeType = totalNumberOfEdgeType;
  }

  public int getWroteEdgeType() {
    return this.wroteEdgeType;
  }


  public int getTotalNumberOfIndices() {
    return this.totalNumberOfIndices;
  }

  public void setTotalNumberOfIndices(int totalNumberOfIndices) {
    this.totalNumberOfIndices = totalNumberOfIndices;
  }

  public int getWroteIndices() {
    return this.wroteIndices;
  }


  public Date getStartWork3Time() {
    return startWork3Time;
  }


  public void setStartWork3Time(Date startWork3Time) {
    this.startWork3Time = startWork3Time;
  }


  public int getTotalNumberOfRecords() {
    return totalNumberOfRecords;
  }


  public void setTotalNumberOfRecords(int totalNumberOfRecords) {
    this.totalNumberOfRecords = totalNumberOfRecords;
  }


  public int getImportedRecords() {
    return importedRecords;
  }



  /*
   *  Incrementing methods
   */


  public Date getStartWork4Time() {
    return startWork4Time;
  }


  public void setStartWork4Time(Date startWork4Time) {
    this.startWork4Time = startWork4Time;
  }


  public void incrementBuiltEntities() {
    this.builtEntities++;
    this.notifyListeners(1);
  }

  public void incrementDoneEntity4Relationship() {
    this.doneEntity4Relationship++;
    this.notifyListeners(1);
  }

  public void incrementBuiltModelVertexTypes() {
    this.builtModelVertexTypes++;
    this.notifyListeners(2);
  }

  public void incrementBuiltModelEdgeTypes() {
    this.builtModelEdgeTypes++;
    this.notifyListeners(2);
  }

  public void incrementWroteVertexType() {
    this.wroteVertexType++;
    this.notifyListeners(3);
  }

  public void incrementWroteEdgeType() {
    this.wroteEdgeType++;
    this.notifyListeners(3);
  }

  public void incrementWroteIndices() {
    this.wroteIndices++;
    this.notifyListeners(3);
  }

  public void incrementImportedRecords(int importedRecord) {
    this.importedRecords += importedRecord;
    this.notifyListeners(4);
  }
  
 
  
  /*
   *  toString methods
   */

  public String sourceDbSchemaBuildingProgress() {
    String s ="";
    s += "Built Entities: " + this.builtEntities + "/" + this.totalNumberOfEntities;
    s += "\nExplored Entities for Relationship: " + this.doneEntity4Relationship + "/" + this.totalNumberOfEntities;
    return s;
  }

  public String graphModelBuildingProgress() {
    String s ="";
    s += "Built Model Vertices: " + this.builtModelVertexTypes + "/" + this.totalNumberOfModelVertices;
    s += "\nBuilt Model Edges: " + this.builtModelEdgeTypes + "/" + this.totalNumberOfModelEdges;
    return s;
  }

  public String orientSchemaWritingProgress() {
    String s ="";
    s += "Wrote Vertex Type: " + this.wroteVertexType + "/" + this.totalNumberOfVertexType;
    s += "\nWrote Edge Type: " + this.wroteEdgeType + "/" + this.totalNumberOfEdgeType;
    s += "\nWrote Indices: " + this.wroteIndices + "/" + this.totalNumberOfIndices;
    return s;
  }

  public String importingProgress() {
    String s ="";
    s += "Imported Records: " + this.importedRecords + "/" + this.totalNumberOfRecords;
    return s;
  }
  
  public String toString() {
    String s = "";
    s += this.sourceDbSchemaBuildingProgress() + "\n\n" + this.graphModelBuildingProgress() + "\n\n" + this.orientSchemaWritingProgress() + "\n\n" + this.importingProgress();
    return s;
  }



}
