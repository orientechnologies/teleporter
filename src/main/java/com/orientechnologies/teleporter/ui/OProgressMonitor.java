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

package com.orientechnologies.teleporter.ui;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.util.OFunctionsHandler;
import java.util.Date;

/**
 * Listener class of ODrakkarStatistics which updates and visualizes a progress monitor.
 *
 * <p>Source DB Schema building: 100% [..................................................] Elapsed:
 * 00:00:00 Remaining: 00:00:00 Warnings: 0
 *
 * <p>Graph Model building: 100% [..................................................] Elapsed:
 * 00:00:00 Remaining: 00:00:00 Warnings: 3
 *
 * <p>OrientDB Schema writing: 100% [..................................................] Elapsed:
 * 00:00:00 Remaining: 00:00:00 Warnings: 5
 *
 * <p>OrientDB importing: 90% [........................................... ] Elapsed: 00:00:00
 * Remaining: 00:00:00 Warnings: 5
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OProgressMonitor implements OStatisticsListener {

  private final String work1Title;
  private final String work2Title;
  private final String work3Title;
  private final String work4Title;
  private final String work5Title;
  private boolean firstPrint;

  /** initialize progress bar properties. */
  public OProgressMonitor() {
    this.work1Title = String.format("%-35s", "(1/4) Source DB Schema building:");
    this.work2Title = String.format("%-35s", "(2/4) Graph Model building:");
    this.work3Title = String.format("%-35s", "(3/4) OrientDB Schema writing:");
    this.work4Title = String.format("%-35s", "(4/4) OrientDB importing:");
    this.work5Title = String.format("%-35s", "Building accessory edges in OrientDB:");
    this.firstPrint = true;
  }

  /**
   * Called whenever the progress monitor needs to be updated. that is whenever progress
   * OTeleporterStatistics publishes an event.
   *
   * @param statistics
   */
  public String updateOnEvent(OTeleporterStatistics statistics) {

    if (firstPrint) {
      OTeleporterContext.getInstance().getMessageHandler().info(this, "\n");
      this.firstPrint = false;
    }

    String message = null;

    switch (statistics.runningStepNumber) {
      case 1:
        message = this.updateWork1OnEvent(statistics);
        break;
      case 2:
        message = this.updateWork2OnEvent(statistics);
        break;
      case 3:
        message = this.updateWork3OnEvent(statistics);
        break;
      case 4:
        message = this.updateWork4OnEvent(statistics);
        break;
      case 5:
        message = this.updateWork5OnEvent(statistics);
        break;
    }
    return message;
  }

  /*
   * Work1: Source DB schema Building
   */

  public String updateWork1OnEvent(OTeleporterStatistics statistics) {

    Date currentTime = new Date();

    int work1DonePercentage =
        (int)
            ((((double) statistics.builtEntities / (double) statistics.totalNumberOfEntities)
                    * 0.25
                    * 100)
                + (((double) statistics.entitiesAnalyzedForRelationship
                        / (double) statistics.totalNumberOfEntities)
                    * 0.75
                    * 100));

    String progressBarWork1 = this.getProgressBar(work1DonePercentage);

    // Time
    long elapsedTime = (currentTime.getTime() - statistics.startWork1Time.getTime());

    return this.printProgressBar(
        this.work1Title,
        work1DonePercentage,
        progressBarWork1,
        elapsedTime,
        statistics.warningMessages.size(),
        -1,
        -1);
  }

  /*
   * Work2: Graph Model Building
   */

  public String updateWork2OnEvent(OTeleporterStatistics statistics) {

    Date currentTime = new Date();

    int work2DonePercentage;

    if (statistics.totalNumberOfModelVertices > 0 && statistics.totalNumberOfModelEdges > 0) {
      work2DonePercentage =
          (int)
              (((double) statistics.builtModelVertexTypes
                      / (double) statistics.totalNumberOfModelVertices)
                  * 100
                  / 2);
      work2DonePercentage +=
          (int)
              (((double) statistics.builtModelEdgeTypes
                      / (double) statistics.totalNumberOfModelEdges)
                  * 100
                  / 2);
    } else if (statistics.totalNumberOfModelVertices > 0
        && statistics.totalNumberOfModelEdges == 0) {
      work2DonePercentage =
          (int)
              (((double) statistics.builtModelVertexTypes
                      / (double) statistics.totalNumberOfModelVertices)
                  * 100);
    } else {
      work2DonePercentage = 0;
    }

    String progressBarWork2 = this.getProgressBar(work2DonePercentage);

    // Time
    long elapsedTime = (currentTime.getTime() - statistics.startWork2Time.getTime());

    return this.printProgressBar(
        this.work2Title,
        work2DonePercentage,
        progressBarWork2,
        elapsedTime,
        statistics.warningMessages.size(),
        -1,
        -1);
  }

  /*
   * Work3: OrientDB Schema Writing
   */

  public String updateWork3OnEvent(OTeleporterStatistics statistics) {

    Date currentTime = new Date();

    int work3DonePercentage;

    if (statistics.totalNumberOfVertexTypes > 0
        && statistics.totalNumberOfEdgeTypes > 0
        && statistics.totalNumberOfIndices > 0) {
      work3DonePercentage =
          (int)
              (((double) statistics.wroteVertexType / (double) statistics.totalNumberOfVertexTypes)
                  * 0.35
                  * 100);
      work3DonePercentage +=
          (int)
              (((double) statistics.wroteEdgeType / (double) statistics.totalNumberOfEdgeTypes)
                  * 0.35
                  * 100);
      work3DonePercentage +=
          (int)
              (((double) statistics.wroteIndexes / (double) statistics.totalNumberOfIndices)
                  * 0.3
                  * 100);
    } else if (statistics.totalNumberOfVertexTypes > 0
        && statistics.totalNumberOfEdgeTypes > 0
        && statistics.totalNumberOfIndices == 0) {
      work3DonePercentage =
          (int)
              (((double) statistics.wroteVertexType / (double) statistics.totalNumberOfVertexTypes)
                  * 100
                  / 2);
      work3DonePercentage +=
          (int)
              (((double) statistics.wroteEdgeType / (double) statistics.totalNumberOfEdgeTypes)
                  * 100
                  / 2);
    } else if (statistics.totalNumberOfVertexTypes > 0
        && statistics.totalNumberOfEdgeTypes == 0
        && statistics.totalNumberOfIndices > 0) {
      work3DonePercentage =
          (int)
              (((double) statistics.wroteVertexType / (double) statistics.totalNumberOfVertexTypes)
                  * 100
                  / 2);
      work3DonePercentage +=
          (int)
              (((double) statistics.wroteIndexes / (double) statistics.totalNumberOfIndices)
                  * 100
                  / 2);
    } else if (statistics.totalNumberOfVertexTypes > 0
        && statistics.totalNumberOfEdgeTypes == 0
        && statistics.totalNumberOfIndices == 0) {
      work3DonePercentage =
          (int)
              (((double) statistics.wroteVertexType / (double) statistics.totalNumberOfVertexTypes)
                  * 100);
    } else {
      work3DonePercentage = 0;
    }

    String progressBarWork3 = this.getProgressBar(work3DonePercentage);
    ;

    // Time
    long elapsedTime = (currentTime.getTime() - statistics.startWork3Time.getTime());

    return this.printProgressBar(
        this.work3Title,
        work3DonePercentage,
        progressBarWork3,
        elapsedTime,
        statistics.warningMessages.size(),
        -1,
        -1);
  }

  /*
   * Work4: OrientDB Importing
   */

  public String updateWork4OnEvent(OTeleporterStatistics statistics) {

    Date currentTime = new Date();

    int work4DonePercentage;
    if (statistics.totalNumberOfEntities > 0) {
      work4DonePercentage =
          (int)
              (((double) statistics.analyzedRecords / (double) statistics.totalNumberOfRecords)
                  * 100);
    } else {
      work4DonePercentage = 0;
    }

    String progressBarWork4 = this.getProgressBar(work4DonePercentage);

    // Time
    long elapsedTime = (currentTime.getTime() - statistics.startWork4Time.getTime());

    return this.printProgressBar(
        this.work4Title,
        work4DonePercentage,
        progressBarWork4,
        elapsedTime,
        statistics.warningMessages.size(),
        statistics.analyzedRecords,
        statistics.totalNumberOfRecords);
  }

  /** Work5: Building edges on join attributes */
  public String updateWork5OnEvent(OTeleporterStatistics statistics) {

    Date currentTime = new Date();
    int numberOfLogicalRelationships = statistics.totalNumberOfLogicalRelationships;

    // TO CHECK

    int work5DonePercentage;
    if (numberOfLogicalRelationships > 0) {

      int work5percentage1;
      if (statistics.leftVerticesCurrentLogicalRelationship > 0) {
        work5percentage1 =
            statistics.doneLeftVerticesCurrentLogicalRelationship
                / statistics.leftVerticesCurrentLogicalRelationship;
      } else {
        work5percentage1 = 0;
      }
      int work5percentage2 =
          statistics.doneLogicalRelationships / statistics.totalNumberOfLogicalRelationships;

      if (work5percentage2 == 1) {
        work5DonePercentage = 100;
      } else {
        work5DonePercentage =
            (work5percentage2 + (work5percentage1 / numberOfLogicalRelationships)) * 100;
      }

    } else {
      work5DonePercentage = 0;
    }

    String progressBarWork5 = this.getProgressBar(work5DonePercentage);

    // Time
    long elapsedTime = (currentTime.getTime() - statistics.startWork5Time.getTime());

    return this.printProgressBar(
        this.work5Title,
        work5DonePercentage,
        progressBarWork5,
        elapsedTime,
        statistics.warningMessages.size(),
        -1,
        -1);
  }

  public void initialize() {
    OTeleporterContext.getInstance().getStatistics().registerListener(this);
  }

  public String printProgressBar(
      String workTitle,
      int workDonePercentage,
      String progressBarWork,
      long elapsedTime,
      int occurredWarnings,
      int importedRecords,
      int totalRecords) {

    String format;
    if (importedRecords == -1 && totalRecords == -1) format = "\r%s %3d%% %s %s %s %s %s %s %s";
    else format = "\r%s %3d%% %s %s %s %s %s %s %s %s %s";

    // Times
    String elapsedHMSTime = OFunctionsHandler.getHMSFormat(elapsedTime);

    long remainingTime;
    if (workDonePercentage > 0)
      remainingTime = (elapsedTime * (long) (100 - workDonePercentage)) / (long) workDonePercentage;
    else remainingTime = 0;
    String remainingHMSTime = OFunctionsHandler.getHMSFormat(remainingTime);

    String message =
        String.format(
            format,
            workTitle,
            workDonePercentage,
            progressBarWork,
            " Elapsed:",
            elapsedHMSTime,
            " Remaining:",
            remainingHMSTime,
            " Warnings:",
            occurredWarnings,
            " Records:",
            importedRecords + "/" + totalRecords);
    OTeleporterContext.getInstance().getMessageHandler().info(this, message);

    if (message.length() > 500) {
      System.out.println();
    }

    return message;
  }

  public String getProgressBar(int workDonePercentage) {

    int pointCharsWork = (workDonePercentage / 5);
    int emptyCharsWork = 20 - pointCharsWork;

    String progressBarWork = "[";
    while (pointCharsWork > 0) {
      progressBarWork += '.';
      pointCharsWork--;
    }

    while (emptyCharsWork > 0) {
      progressBarWork += ' ';
      emptyCharsWork--;
    }

    progressBarWork += "]";

    return progressBarWork;
  }
}
