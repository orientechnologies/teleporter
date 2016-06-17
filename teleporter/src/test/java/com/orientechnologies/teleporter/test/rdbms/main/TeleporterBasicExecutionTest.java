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

package com.orientechnologies.teleporter.test.rdbms.main;

import com.orientechnologies.teleporter.main.OTeleporter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 *
 */

public class TeleporterBasicExecutionTest extends TeleporterInvocationTest {

  @Before
  public void init() {
    buildEnvironmentForExecution();
    prepareArguments();
    prepareArrayArgs();
  }

  @Test
  public void test1() {

    boolean jobComplete = false;
    int retry = 0;

    while(!jobComplete) {

      try {
        OTeleporter.main(args);
        jobComplete = true;
      } catch (Exception e) {
        e.printStackTrace();
        if(retry < 3) {
          System.out.printf("Job failed, restarting job (retry " + (retry+1) + "/3)");
          retry++;
          continue;
        }
        else {
          fail("Job failed " + (retry + 1) + " times!\n" + e.getMessage());
        }
      } finally {
        shutdownEnvironment();
      }
    }

  }


}
