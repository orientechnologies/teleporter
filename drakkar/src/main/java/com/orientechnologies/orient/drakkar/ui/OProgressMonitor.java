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

package com.orientechnologies.orient.drakkar.ui;

import java.util.Date;

import com.orientechnologies.orient.drakkar.context.ODrakkarStatistics;
import com.orientechnologies.orient.drakkar.util.OTimeFormatHandler;

/**
 * 
 * Listener class of ODrakkarStatistics which updates and visualizes a progress monitor. 
 * 
 * Source DB Schema building: 100% [..................................................]
 * 
 * Graph Model building:       50% [.........................                         ]
 * 
 * OrientDB Schema writing:     0% [                                                  ]
 * 
 * OrientDB importing:         90% [                                                  ]
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class OProgressMonitor implements OStatisticsListener {

  private final String work1Title;
  private final String work2Title;
  private final String work3Title;
  private final String work4Title;

  /**
   * initialize progress bar properties.
   */
  public OProgressMonitor() {
    this.work1Title = "(1/4) Source DB Schema building: ";
    this.work2Title = "(2/4) Graph Model building:      ";
    this.work3Title = "(3/4) OrientDB Schema writing:   ";
    this.work4Title = "(4/4) OrientDB importing:        ";
  }

  public void updateOnEvent(ODrakkarStatistics statistics, int workNumber) {

    switch(workNumber) {
    case 1: this.updateWork1OnEvent(statistics);
    break;
    case 2: this.updateWork2OnEvent(statistics);
    break;
    case 3: this.updateWork3OnEvent(statistics);
    break;
    case 4: this.updateWork4OnEvent(statistics);
    break;
    }
  }

  /**
   * Called whenever the progress monitor needs to be updated.
   * that is whenever progress ODrakkarStatistics publishes an event.
   *
   * @param statistics 
   */
  public void updateWork1OnEvent(ODrakkarStatistics statistics) {

    /*
     * Work1: Source DB schema Building
     */

    Date currentTime = new Date();

    int work1DonePercentage = (int)( (((double)statistics.getBuiltEntities()/(double)statistics.getTotalNumberOfEntities()) * 0.25 * 100) + 
        (((double)statistics.getDoneEntity4Relationship()/(double)statistics.getTotalNumberOfEntities()) * 0.75 * 100) );

    int pointCharsWork1 = (work1DonePercentage/2);
    int emptyCharsWork1 = 50-pointCharsWork1;

    String progressBarWork1 = "[";
    while (pointCharsWork1 > 0) {
      progressBarWork1 += '.';
      pointCharsWork1--;
    }

    while (emptyCharsWork1 > 0) {
      progressBarWork1 += ' ';
      emptyCharsWork1--;
    }

    progressBarWork1 += "]";    

    // Time
    long elapsedTime = (currentTime.getTime() - statistics.getStartWork1Time().getTime());

    this.printProgressBar(this.work1Title, work1DonePercentage, progressBarWork1, elapsedTime);
  }

  public void updateWork2OnEvent(ODrakkarStatistics statistics) {

    /*
     * Work2: Graph Model Building
     */

    Date currentTime = new Date();

    int work2DonePercentage;
    if(statistics.getTotalNumberOfModelVertices() > 0) {
      work2DonePercentage = (int) (((double)statistics.getBuiltModelVertexTypes()/(double)statistics.getTotalNumberOfModelVertices()) * 0.5 * 100);
      if(statistics.getTotalNumberOfModelEdges() > 0)
        work2DonePercentage += (int) (((double)statistics.getBuiltModelEdgeTypes()/(double)statistics.getTotalNumberOfModelEdges()) * 0.5 * 100);

    }
    else {
      work2DonePercentage = 0;
    }

    int pointCharsWork2 = (work2DonePercentage/2);
    int emptyCharsWork2 = 50-pointCharsWork2;

    String progressBarWork2 = "[";
    while (pointCharsWork2 > 0) {
      progressBarWork2 += '.';
      pointCharsWork2--;
    }

    while (emptyCharsWork2 > 0) {
      progressBarWork2 += ' ';
      emptyCharsWork2--;
    }

    progressBarWork2 += "]";

    // Time
    long elapsedTime = (currentTime.getTime() - statistics.getStartWork2Time().getTime());
    
    this.printProgressBar(this.work2Title, work2DonePercentage, progressBarWork2, elapsedTime);
  }


  public void updateWork3OnEvent(ODrakkarStatistics statistics) {
    /*
     * Work3: OrientDB Schema Writing
     */

    Date currentTime = new Date();

    int work3DonePercentage;

    if(statistics.getTotalNumberOfVertexType() > 0) {
      work3DonePercentage = (int) (((double)statistics.getWroteVertexType()/(double)statistics.getTotalNumberOfVertexType()) * 0.3 * 100);
      if(statistics.getTotalNumberOfEdgeType() > 0)
        work3DonePercentage += (int) (((double)statistics.getWroteEdgeType()/(double)statistics.getTotalNumberOfEdgeType()) * 0.3 * 100);
      if(statistics.getTotalNumberOfIndices() > 0)
        work3DonePercentage += (int) (((double)statistics.getWroteIndices()/(double)statistics.getTotalNumberOfIndices()) * 0.4 * 100);      

    }
    else {
      work3DonePercentage = 0;
    }

    int pointCharsWork3 = (work3DonePercentage/2);
    int emptyCharsWork3 = 50-pointCharsWork3;

    String progressBarWork3 = "[";
    while (pointCharsWork3 > 0) {
      progressBarWork3 += '.';
      pointCharsWork3--;
    }

    while (emptyCharsWork3 > 0) {
      progressBarWork3 += ' ';
      emptyCharsWork3--;
    }

    progressBarWork3 += "]";

    // Time
    long elapsedTime = (currentTime.getTime() - statistics.getStartWork3Time().getTime());

    this.printProgressBar(this.work3Title, work3DonePercentage, progressBarWork3, elapsedTime);
  }

  public void updateWork4OnEvent(ODrakkarStatistics statistics) {

    /*
     * Work4: OrientDB Importing
     */

    Date currentTime = new Date();

    int work4DonePercentage;
    if(statistics.getTotalNumberOfEntities() > 0) {
      work4DonePercentage = (int) (((double)statistics.getImportedRecords()/(double)statistics.getTotalNumberOfRecords()) * 100);
    }
    else {
      work4DonePercentage = 0;
    }

    int pointCharsWork4 = (work4DonePercentage/2);
    int emptyCharsWork4 = 50-pointCharsWork4;

    String progressBarWork4 = "[";
    while (pointCharsWork4 > 0) {
      progressBarWork4 += '.';
      pointCharsWork4--;
    }

    while (emptyCharsWork4 > 0) {
      progressBarWork4 += ' ';
      emptyCharsWork4--;
    }

    progressBarWork4 += "]";

    // Time
    long elapsedTime = (currentTime.getTime() - statistics.getStartWork4Time().getTime());

    this.printProgressBar(this.work4Title, work4DonePercentage, progressBarWork4, elapsedTime);
  }


  public void initialize(ODrakkarStatistics statistics) {
    statistics.registerListener(this);    
  }

  public void printProgressBar(String workTitle, int workDonePercentage, String progressBarWork, long elapsedTime) {

    String format = "\r%s %3d%% %s %s %s %s %s";
    
    // Times
    String elapsedHMSTime = OTimeFormatHandler.getHMSFormat(elapsedTime);
    
    long remainingTime;
    if(workDonePercentage > 0)
      remainingTime = (elapsedTime*(long)(100-workDonePercentage))/(long)workDonePercentage;
    else
      remainingTime = 0;
    String remainingHMSTime = OTimeFormatHandler.getHMSFormat(remainingTime);
    
    System.out.printf(format, workTitle, workDonePercentage, progressBarWork, "\tElapsed Time: ", elapsedHMSTime, "\tRemaining Time: ", remainingHMSTime);
    
    if(workDonePercentage == 100) {
      System.out.println();
    }
    
  }
}