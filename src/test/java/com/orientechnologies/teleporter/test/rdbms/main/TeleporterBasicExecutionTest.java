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

package com.orientechnologies.teleporter.test.rdbms.main;

import com.orientechnologies.teleporter.main.OTeleporter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
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

    while (!jobComplete) {

      try {
        OTeleporter.main(args);
        jobComplete = true;
      } catch (Exception e) {
        e.printStackTrace();
        if (retry < 3) {
          System.out.printf("Job failed, restarting job (retry " + (retry + 1) + "/3)");
          retry++;
          continue;
        } else {
          fail("Job failed " + (retry + 1) + " times!\n" + e.getMessage());
        }
      } finally {
        shutdownEnvironment();
      }
    }

  }

}
