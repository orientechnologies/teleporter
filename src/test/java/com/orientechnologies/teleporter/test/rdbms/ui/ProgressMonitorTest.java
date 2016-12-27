/*
 * Copyright 2016 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.teleporter.test.rdbms.ui;

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.ui.OProgressMonitor;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class ProgressMonitorTest {

  private OTeleporterContext    context;
  private OTeleporterStatistics statistics;
  private OOutputStreamManager  outputManager;
  private OProgressMonitor      progressMonitor;

  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance();
    this.statistics = new OTeleporterStatistics();
    this.context.setStatistics(this.statistics);
    this.outputManager = new OOutputStreamManager(2);
    this.context.setOutputManager(outputManager);
    this.progressMonitor = new OProgressMonitor();
  }

  //  @Ignore
  @Test
  public void firstPhaseBarTest() {

    String work1Title = String.format("%-35s", "(1/4) Source DB Schema building:");
    String format = "\r%s %3d%% %s %s %s %s %s %s %s";
    statistics.warningMessages = new HashSet<String>();

    statistics.runningStepNumber = 1;
    statistics.builtEntities = 10;
    statistics.totalNumberOfEntities = 10;
    statistics.entitiesAnalyzedForRelationship = 5;
    statistics.startWork1Time = new Date();

    // it must print 62%
    String messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    String progressBarWork = progressMonitor.getProgressBar(62);
    String manuallyBuiltMessage = String
        .format(format, work1Title, 62, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0");

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println("\n");

  }

  @Test
  public void secondPhaseBarTest() {

    String work2Title = String.format("%-35s", "(2/4) Graph Model building:");
    String format = "\r%s %3d%% %s %s %s %s %s %s %s";
    statistics.warningMessages = new HashSet<String>();

    // statistics.totalNumberOfModelVertices > 0 && statistics.totalNumberOfRelationships > 0
    statistics.runningStepNumber = 2;
    statistics.builtModelVertexTypes = 5;
    statistics.totalNumberOfModelVertices = 10;
    statistics.totalNumberOfRelationships = 0;
    statistics.startWork2Time = new Date();

    // it must print 50%
    String messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    String progressBarWork = progressMonitor.getProgressBar(50);
    String manuallyBuiltMessage = String
        .format(format, work2Title, 50, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0");

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println();

    // statistics.totalNumberOfModelVertices > 0 && statistics.totalNumberOfRelationships == 0
    statistics.builtModelVertexTypes = 4;
    statistics.totalNumberOfModelVertices = 16;
    statistics.totalNumberOfRelationships = 0;
    statistics.startWork2Time = new Date();

    // it must print 25%
    messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    progressBarWork = progressMonitor.getProgressBar(25);
    manuallyBuiltMessage = String
        .format(format, work2Title, 25, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0");

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println();

    // statistics.totalNumberOfModelVertices == 0 && statistics.totalNumberOfRelationships == 0
    statistics.builtModelVertexTypes = 0;
    statistics.totalNumberOfModelVertices = 0;
    statistics.totalNumberOfRelationships = 0;
    statistics.startWork2Time = new Date();

    // it must print 0%
    messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    progressBarWork = progressMonitor.getProgressBar(0);
    manuallyBuiltMessage = String
        .format(format, work2Title, 0, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0");

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println("\n");

  }

  @Test
  public void thirdPhaseBarTest() {

    String work3Title = String.format("%-35s", "(3/4) OrientDB Schema writing:");
    String format = "\r%s %3d%% %s %s %s %s %s %s %s";
    statistics.warningMessages = new HashSet<String>();

    // statistics.totalNumberOfVertexTypes > 0 && statistics.totalNumberOfModelEdges > 0 && statistics.totalNumberOfIndices == 0
    statistics.runningStepNumber = 3;
    statistics.wroteVertexType = 5;
    statistics.totalNumberOfVertexTypes = 5;
    statistics.wroteEdgeType = 5;
    statistics.totalNumberOfEdgeTypes = 10;
    statistics.wroteIndexes = 0;
    statistics.totalNumberOfIndices = 0;
    statistics.startWork3Time = new Date();

    // it must print 75%
    String messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    String progressBarWork = progressMonitor.getProgressBar(75);
    String manuallyBuiltMessage = String
        .format(format, work3Title, 75, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0");

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println();

    // statistics.totalNumberOfVertexTypes > 0 && statistics.totalNumberOfModelEdges == 0 && statistics.totalNumberOfIndices > 0
    statistics.wroteVertexType = 5;
    statistics.totalNumberOfVertexTypes = 5;
    statistics.wroteEdgeType = 0;
    statistics.totalNumberOfEdgeTypes = 0;
    statistics.wroteIndexes = 2;
    statistics.totalNumberOfIndices = 5;
    statistics.startWork3Time = new Date();

    // it must print 70%
    messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    progressBarWork = progressMonitor.getProgressBar(70);
    manuallyBuiltMessage = String
        .format(format, work3Title, 70, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0");

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println();

    // statistics.totalNumberOfVertexTypes > 0 && statistics.totalNumberOfModelEdges > 0 && statistics.totalNumberOfIndices > 0
    statistics.wroteVertexType = 10;
    statistics.totalNumberOfVertexTypes = 10;
    statistics.wroteEdgeType = 8;
    statistics.totalNumberOfEdgeTypes = 8;
    statistics.wroteIndexes = 5;
    statistics.totalNumberOfIndices = 10;
    statistics.startWork3Time = new Date();

    // it must print 85%
    messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    progressBarWork = progressMonitor.getProgressBar(85);
    manuallyBuiltMessage = String
        .format(format, work3Title, 85, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0");

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println();

    // statistics.totalNumberOfVertexTypes > 0 && statistics.totalNumberOfModelEdges == 0 && statistics.totalNumberOfIndices == 0
    statistics.wroteVertexType = 3;
    statistics.totalNumberOfVertexTypes = 5;
    statistics.wroteEdgeType = 0;
    statistics.totalNumberOfEdgeTypes = 0;
    statistics.wroteIndexes = 0;
    statistics.totalNumberOfIndices = 0;
    statistics.startWork3Time = new Date();

    // it must print 60%
    messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    progressBarWork = progressMonitor.getProgressBar(60);
    manuallyBuiltMessage = String
        .format(format, work3Title, 60, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0");

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println();

    // statistics.totalNumberOfVertexTypes == 0 && statistics.totalNumberOfModelEdges == 0 && statistics.totalNumberOfIndices == 0
    statistics.wroteVertexType = 0;
    statistics.totalNumberOfVertexTypes = 0;
    statistics.wroteEdgeType = 0;
    statistics.totalNumberOfEdgeTypes = 0;
    statistics.wroteIndexes = 0;
    statistics.totalNumberOfIndices = 0;
    statistics.startWork3Time = new Date();

    // it must print 0%
    messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    progressBarWork = progressMonitor.getProgressBar(0);
    manuallyBuiltMessage = String
        .format(format, work3Title, 0, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0");

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println("\n");
  }

  @Test
  public void fourthPhaseBarTest() {

    String work4Title = String.format("%-35s", "(4/4) OrientDB importing:");
    String format = "\r%s %3d%% %s %s %s %s %s %s %s %s %s";
    statistics.warningMessages = new HashSet<String>();

    // statistics.totalNumberOfEntities > 0
    statistics.runningStepNumber = 4;
    statistics.totalNumberOfEntities = 10;
    statistics.analyzedRecords = 500;
    statistics.totalNumberOfRecords = 1000;
    statistics.startWork4Time = new Date();

    // it must print 50%
    String messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    String progressBarWork = progressMonitor.getProgressBar(50);
    String manuallyBuiltMessage = String
        .format(format, work4Title, 50, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0",
            " Records:", statistics.analyzedRecords + "/" + statistics.totalNumberOfRecords);

    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println();

    // statistics.totalNumberOfEntities == 0
    statistics.runningStepNumber = 4;
    statistics.totalNumberOfEntities = 0;
    statistics.startWork4Time = new Date();

    // it must print 0%
    messageFromProseeMonitor = this.progressMonitor.updateOnEvent(this.statistics);
    progressBarWork = progressMonitor.getProgressBar(0);
    manuallyBuiltMessage = String
        .format(format, work4Title, 0, progressBarWork, " Elapsed:", "00:00:00", " Remaining:", "00:00:00", " Warnings:", "0",
            " Records:", statistics.analyzedRecords + "/" + statistics.totalNumberOfRecords);
    assertEquals(messageFromProseeMonitor, manuallyBuiltMessage);
    System.out.println("\n");

  }

}
