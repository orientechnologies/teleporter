/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.teleporter.test.rdbms.mappingexport;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSModelBuildingAggregationStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.OAbstractDBMSModelBuildingStrategy;
import com.orientechnologies.teleporter.util.ODocumentComparator;
import com.orientechnologies.teleporter.util.OFileManager;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */

public class InteractiveStrategyTest {

  private final String config = "src/test/resources/configuration-mapping/config-handler-output.json";
  private OTeleporterContext   context;
  private ODBMSDataTypeHandler dataTypeHandler;

  private OER2GraphMapper mapper;
  private ODBQueryEngine  dbQueryEngine;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private String                     outOrientGraphUri;
  private OSourceDatabaseInfo        sourceDBInfo;
  private OAbstractDBMSModelBuildingStrategy strategy;
  private String outParentDirectory = "embedded:target/";

  @Before
  public void init() {
    this.outOrientGraphUri = "embedded:target/testOrientDB";
    this.context = OTeleporterContext.newInstance(this.outParentDirectory);
    this.context.setExecutionStrategy("interactive-aggr");
    this.dataTypeHandler = new OHSQLDBDataTypeHandler();
    this.context.setDataTypeHandler(dataTypeHandler);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setMessageHandler(new OTeleporterMessageHandler(0));
    this.sourceDBInfo = new OSourceDatabaseInfo("hsqldb", this.driver, this.jurl, this.username, this.password);
    this.strategy = new ODBMSModelBuildingAggregationStrategy();
  }

  @Test
  /**
   * Testing:
   * - Interactive strategy with aggregation
   */ public void test1() {

    OConfigurationHandler configurationHandler = new OConfigurationHandler(true);
    Connection connection = null;
    Statement st = null;

    try {

      /**
       * Graph model building
       */

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String departmentTableBuilding =
          "create memory table DEPARTMENT (ID varchar(256) not null, DEPARTMENT_NAME  varchar(256) not null,"
              + " LOCATION varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(departmentTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, SALARY double not null,"
          + " EMAIL varchar(256) not null, DEPARTMENT varchar(256) not null, primary key (ID),"
          + " foreign key (DEPARTMENT) references DEPARTMENT(ID))";
      st.execute(employeeTableBuilding);

      String projectTableBuilding = "create memory table PROJECT (ID varchar(256) not null, PROJECT_NAME  varchar(256),"
          + " DESCRIPTION varchar(256) not null, START_DATE date not null, EXPECTED_END_DATE date not null, primary key (ID))";
      st.execute(projectTableBuilding);

      String projectEmployeeTableBuilding =
          "create memory table EMPLOYEE_PROJECT (EMPLOYEE_ID  varchar(256)not null, PROJECT_ID varchar(256) not null,"
              + " ROLE varchar(256) not null, primary key (EMPLOYEE_ID, PROJECT_ID), foreign key (EMPLOYEE_ID) references EMPLOYEE(ID),"
              + " foreign key (PROJECT_ID) references PROJECT(ID))";
      st.execute(projectEmployeeTableBuilding);

      ODocument executionResult = this.strategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null);

      /**
       * Testing JSON building
       */

      ODocument inputConfigurationDoc = null;
      try {
        inputConfigurationDoc = OFileManager.buildJsonFromFile(this.config);
      } catch (IOException e) {
        e.printStackTrace();
        fail();
      }

      assertTrue(ODocumentComparator.areEquals(inputConfigurationDoc, executionResult));

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }
  }

}
