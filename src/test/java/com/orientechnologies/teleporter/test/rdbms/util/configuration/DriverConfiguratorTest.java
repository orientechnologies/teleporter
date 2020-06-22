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

package com.orientechnologies.teleporter.test.rdbms.util.configuration;

import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.output.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.util.ODriverConfigurator;
import com.orientechnologies.teleporter.util.OFileManager;
import java.io.IOException;
import org.junit.Before;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class DriverConfiguratorTest {

  private OTeleporterContext context;
  private ODriverConfigurator driverConfigurator;
  public static final String DRIVERS =
      "http://orientdb.com/jdbc-drivers.json"; // it must be coherent with the instance variable
  // "DRIVERS" of the class ODriverConfigurator
  private OOutputStreamManager outputManager;
  private String fileName;
  private String outParentDirectory = "embedded:target/";

  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance(outParentDirectory);
    this.driverConfigurator = new ODriverConfigurator();
    this.outputManager = new OOutputStreamManager(2);
    this.context.setMessageHandler(new OTeleporterMessageHandler(0));

    ODocument driversParams = this.driverConfigurator.readJsonFromRemoteUrl(DRIVERS);
    ODocument hsqldbConfig = driversParams.field("HyperSQL");
    this.fileName = hsqldbConfig.field("url");
    fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
  }

  //  @Test
  public void checkConfigurationTest() {

    try {

      try {
        OFileManager.deleteResource("../lib/" + fileName);
      } catch (IOException e) {
        System.out.println("Driver not present in '../lib/' path.");
      }

      String driverName = this.driverConfigurator.fetchDriverClassName("hypersql");
      this.driverConfigurator.checkDriverConfiguration("hypersql", "lib/");
      OFileManager.deleteResource("lib/");

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  //  @Test
  public void checkConnectionTest() {

    try {
      OFileManager.deleteResource("lib/" + fileName);
    } catch (IOException e) {
      System.out.println("Driver not present in 'lib/' path.");
    }

    try {

      driverConfigurator.checkConnection("HyperSQL", "jdbc:hsqldb:mem:mydb", "SA", "", "lib/");
      OFileManager.deleteResource("lib/");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
