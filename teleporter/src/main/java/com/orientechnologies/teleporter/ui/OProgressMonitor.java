/*
 * Copyright 2015 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.teleporter.ui;

import java.util.Date;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.util.OTimeFormatHandler;

/**
 * 
 * Listener class of ODrakkarStatistics which updates and visualizes a progress monitor. 
 * 
 * Source DB Schema building: 100% [..................................................] Elapsed: 00:00:00 Remaining: 00:00:00 Warnings: 0
 * 
 * Graph Model building:      100% [..................................................] Elapsed: 00:00:00 Remaining: 00:00:00 Warnings: 3
 * 
 * OrientDB Schema writing:   100% [..................................................] Elapsed: 00:00:00 Remaining: 00:00:00 Warnings: 5
 * 
 * OrientDB importing:         90% [...........................................       ] Elapsed: 00:00:00 Remaining: 00:00:00 Warnings: 5
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OProgressMonitor implements OStatisticsListener {

  private OTeleporterContext context;

  private final String work1Title;
  private final String work2Title;
  private final String work3Title;
  private final String work4Title;

  /**
   * initialize progress bar properties.
   */
  public OProgressMonitor(OTeleporterContext context) {
    this.work1Title = String.format("%-35s","(1/4) Source DB Schema building:");
    this.work2Title = String.format("%-35s","(2/4) Graph Model building:");
    this.work3Title = String.format("%-35s","(3/4) OrientDB Schema writing:");
    this.work4Title = String.format("%-35s","(4/4) OrientDB importing:");
    this.context = context;
  }

  public void updateOnEvent(OTeleporterStatistics statistics) {

    switch(statistics.runningStepNumber) {
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
  public void updateWork1OnEvent(OTeleporterStatistics statistics) {

    /*
     * Work1: Source DB schema Building
     */

    Date currentTime = new Date();

    int work1DonePercentage = (int)( (((double)statistics.builtEntities/(double)statistics.totalNumberOfEntities) * 0.25 * 100) + 
        (((double)statistics.doneEntity4Relationship/(double)statistics.totalNumberOfEntities) * 0.75 * 100) );

    int pointCharsWork1 = (work1DonePercentage/5);
    int emptyCharsWork1 = 20-pointCharsWork1;

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
    long elapsedTime = (currentTime.getTime() - statistics.startWork1Time.getTime());

    this.printProgressBar(this.work1Title, work1DonePercentage, progressBarWork1, elapsedTime, statistics.warningMessages.size(), -1, -1);
  }

  public void updateWork2OnEvent(OTeleporterStatistics statistics) {

    /*
     * Work2: Graph Model Building
     */

    Date currentTime = new Date();

    int work2DonePercentage;
    if(statistics.totalNumberOfModelVertices > 0) {
      work2DonePercentage = (int) (((double)statistics.builtModelVertexTypes/(double)statistics.totalNumberOfModelVertices) * 0.5 * 100);
      if(statistics.totalNumberOfRelationships > 0)
        work2DonePercentage += (int) (((double)statistics.analizedRelationships/(double)statistics.totalNumberOfRelationships) * 0.5 * 100);

    }
    else {
      work2DonePercentage = 0;
    }

    int pointCharsWork2 = (work2DonePercentage/5);
    int emptyCharsWork2 = 20-pointCharsWork2;

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
    long elapsedTime = (currentTime.getTime() - statistics.startWork2Time.getTime());

    this.printProgressBar(this.work2Title, work2DonePercentage, progressBarWork2, elapsedTime, statistics.warningMessages.size(), -1, -1);
  }


  public void updateWork3OnEvent(OTeleporterStatistics statistics) {
   
	  /*
     * Work3: OrientDB Schema Writing
     */

    Date currentTime = new Date();

    int work3DonePercentage;

    if(statistics.totalNumberOfVertexType > 0) {
      work3DonePercentage = (int) (((double)statistics.wroteVertexType/(double)statistics.totalNumberOfVertexType) * 0.3 * 100);
      if(statistics.totalNumberOfEdgeType > 0)
        work3DonePercentage += (int) (((double)statistics.wroteEdgeType/(double)statistics.totalNumberOfEdgeType) * 0.3 * 100);
      if(statistics.totalNumberOfIndices > 0)
        work3DonePercentage += (int) (((double)statistics.wroteIndices/(double)statistics.totalNumberOfIndices) * 0.4 * 100);      

    }
    else {
      work3DonePercentage = 0;
    }

    int pointCharsWork3 = (work3DonePercentage/5);
    int emptyCharsWork3 = 20-pointCharsWork3;

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
    long elapsedTime = (currentTime.getTime() - statistics.startWork3Time.getTime());

    this.printProgressBar(this.work3Title, work3DonePercentage, progressBarWork3, elapsedTime, statistics.warningMessages.size(), -1, -1);
  }

  public void updateWork4OnEvent(OTeleporterStatistics statistics) {

    /*
     * Work4: OrientDB Importing
     */

    Date currentTime = new Date();

    int work4DonePercentage;
    if(statistics.totalNumberOfEntities > 0) {
      work4DonePercentage = (int) (((double)statistics.analyzedRecords/(double)statistics.totalNumberOfRecords) * 100);
    }
    else {
      work4DonePercentage = 0;
    }

    int pointCharsWork4 = (work4DonePercentage/5);
    int emptyCharsWork4 = 20-pointCharsWork4;

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
    long elapsedTime = (currentTime.getTime() - statistics.startWork4Time.getTime());

    this.printProgressBar(this.work4Title, work4DonePercentage, progressBarWork4, elapsedTime, statistics.warningMessages.size(), statistics.analyzedRecords, statistics.totalNumberOfRecords);
  }


  public void initialize() {
    context.getStatistics().registerListener(this);    
  }

  public void printProgressBar(String workTitle, int workDonePercentage, String progressBarWork, long elapsedTime, int occurredWarnings, int importedRecords, int totalRecords) {

    String format;
    if(importedRecords  == -1 && totalRecords == -1)
      format = "\r%s %3d%% %s %s %s %s %s %s %s";
    else
      format = "\r%s %3d%% %s %s %s %s %s %s %s %s %s";


    // Times
    String elapsedHMSTime = OTimeFormatHandler.getHMSFormat(elapsedTime);

    long remainingTime;
    if(workDonePercentage > 0)
      remainingTime = (elapsedTime*(long)(100-workDonePercentage))/(long)workDonePercentage;
    else
      remainingTime = 0;
    String remainingHMSTime = OTimeFormatHandler.getHMSFormat(remainingTime);

    context.getOutputManager().info(format, workTitle, workDonePercentage, progressBarWork, " Elapsed:", elapsedHMSTime, " Remaining:", remainingHMSTime, " Warnings:", occurredWarnings, " Records:", importedRecords + "/" + totalRecords);

  }
}