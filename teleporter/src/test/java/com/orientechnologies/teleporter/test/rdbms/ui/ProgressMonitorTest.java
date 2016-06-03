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

/**
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 *
 */

public class ProgressMonitorTest {

  private OTeleporterContext    context;
  private OTeleporterStatistics statistics;
  private OOutputStreamManager  outputManager;
  private OProgressMonitor progressMonitor;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.statistics = new OTeleporterStatistics();
    this.context.setStatistics(this.statistics);
    this.outputManager = new OOutputStreamManager(2);
    this.context.setOutputManager(outputManager);
    this.progressMonitor = new OProgressMonitor(this.context);
  }

  @Test
  public void firstPhaseBarTest() {

    String work1Title = String.format("%-35s","(1/4) Source DB Schema building:");

    statistics.runningStepNumber = 1;
    statistics.builtEntities = 10;
    statistics.totalNumberOfEntities = 10;
    statistics.doneEntity4Relationship = 14;
    statistics.totalNumberOfRelationships = 21;


    // it must print 81%
    String message = this.progressMonitor.updateOnEvent(this.statistics);



  }


  @Test
  public void secondPhaseBarTest() {

  }


  @Test
  public void thirdPhaseBarTest() {

  }


  @Test
  public void fourthPhaseBarTest() {

  }


}
