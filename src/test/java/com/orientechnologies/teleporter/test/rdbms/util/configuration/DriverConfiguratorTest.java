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

package com.orientechnologies.teleporter.test.rdbms.util.configuration;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.util.ODriverConfigurator;
import com.orientechnologies.teleporter.util.OFileManager;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 *
 */

public class DriverConfiguratorTest {

  private OTeleporterContext context;
  private ODriverConfigurator driverConfigurator;
  public static final String DRIVERS = "http://orientdb.com/jdbc-drivers.json";  // it must be coherent with the instance variable "DRIVERS" of the class ODriverConfigurator
  private OOutputStreamManager outputManager;
  private String fileName;

  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance();
    this.driverConfigurator = new ODriverConfigurator();
    this.outputManager = new OOutputStreamManager(2);
    this.context.setOutputManager(outputManager);

    ODocument driversParams = this.driverConfigurator.readJsonFromUrl(DRIVERS);
    ODocument hsqldbConfig = driversParams.field("HyperSQL");
    this.fileName = hsqldbConfig.field("url");
    fileName = fileName.substring(fileName.lastIndexOf("/")+1);
  }

  @Test
  public void checkConfigurationTest() {

    try {

      try {
        OFileManager.deleteResource("../lib/" + fileName);
      }catch(IOException e) {
        System.out.println("Driver not present in '../lib/' path.");
      }

      String driverName = this.driverConfigurator.fetchDriverClassName("hypersql");
      this.driverConfigurator.checkDriverConfiguration("hypersql", "lib/");
      OFileManager.deleteResource("lib/");

    }catch(Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

  }


  @Test
  public void checkConnectionTest() {

    try {
      OFileManager.deleteResource("lib/" + fileName);
    }catch(IOException e) {
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
