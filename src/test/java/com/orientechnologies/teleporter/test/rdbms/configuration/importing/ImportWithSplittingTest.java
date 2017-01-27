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

package com.orientechnologies.teleporter.test.rdbms.configuration.importing;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.orientechnologies.teleporter.util.OFileManager;
import com.orientechnologies.teleporter.util.OGraphCommands;
import com.orientechnologies.teleporter.util.OMigrationConfigManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class ImportWithSplittingTest {

  private OTeleporterContext context;
  private ODBMSNaiveStrategy naiveStrategy;
  private String             dbParentDirectoryPath;
  private final String configPathJson = "src/test/resources/configuration-mapping/splitting-into2tables-mapping.json";
  private ODBQueryEngine dbQueryEngine;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private String dbName = "testOrientDB";
  private String outParentDirectory = "embedded:target/";
  private String outOrientGraphUri = this.outParentDirectory + this.dbName;
  private OSourceDatabaseInfo sourceDBInfo;


  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance();
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.naiveStrategy = new ODBMSNaiveStrategy();
    this.sourceDBInfo = new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
  }

  @After
  public void tearDown() {

    // closing OrientDB instance
    this.context.closeOrientDBInstance();

    try {

      // Deleting database directory
      OFileManager.deleteResource(this.outOrientGraphUri.replace("embedded:",""));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Test
  /*
   *  Source DB schema:
   *
   *  - 1 hsqldb source
   *  - 1 relationship from employee to department (not declared through foreign key definition)
   *  - 3 tables: "employee", "department", "chief_officer"
   *
   *  employee(first_name, last_name, salary, department, project, balance, role)
   *  department(id, name, location, updated_on)
   *  chief_officer(first_name, last_name, project)
   *
   *  Desired Graph Model:
   *
   *  - 4 vertex classes: "Employee" and "Project" (both split from employee entity), "Department", "ChiefOfficer"
   *  - 1 edge class "WorksAt", corresponding to the relationship between  the "employee" as person and "department"
   *  - 1 edge class "HasProject", representing the splitting-edge connecting each couple of instances of "Employee"
   *    and "Project" coming from the same record of the "employee" table. It has a "role" property coming from the
   *    "employee" table too.
   *  - 1 edge class "isChiefForProject", representing the relationship between the CEO and the correspondent project.
   *
   *  Employee(firstName, lastName, salary, department)
   *  Project(project, balance, role)
   *  Department(id, departmentName, location)
   *  ChiefOfficer(firstName, lastName, project)
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String employeeTableBuilding = "create memory table EMPLOYEE_PROJECT (FIRST_NAME varchar(256) not null,"
          + " LAST_NAME varchar(256) not null, SALARY double not null, DEPARTMENT varchar(256) not null,"
          + " PROJECT varchar(256) not null, BALANCE double not null, ROLE varchar(256), primary key (FIRST_NAME,LAST_NAME,PROJECT))";
      st = connection.createStatement();
      st.execute(employeeTableBuilding);

      String departmentTableBuilding = "create memory table DEPARTMENT (ID varchar(256),"
          + " NAME varchar(256) not null, LOCATION varchar(256) not null, UPDATED_ON date not null, primary key (ID))";
      st.execute(departmentTableBuilding);

      String chiefTableBuilding =
          "create memory table CHIEF_OFFICER (FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, "
              + "PROJECT varchar(256) not null, primary key (FIRST_NAME,LAST_NAME))";
      st.execute(chiefTableBuilding);

      // Records Inserting

      String personFilling = "insert into EMPLOYEE_PROJECT (FIRST_NAME,LAST_NAME,SALARY,DEPARTMENT,PROJECT,BALANCE,ROLE) values ("
          + "('Joe','Black','20000','D001','Mars','12000','T')," + "('Thomas','Anderson','35000','D002','Venus','15000','T'),"
          + "('Tyler','Durden','35000','D001','Iuppiter','20000','A'),"
          + "('John','McClanenei','25000','D001','Venus','15000','S')," + "('Marty','McFly','40000','D002','Mars','12000','M'),"
          + "('Marty','McFly','40000','D002','Mercury','5000','M'))";
      st.execute(personFilling);

      String departmentFilling =
          "insert into DEPARTMENT (ID,NAME,LOCATION,UPDATED_ON) values (" + "('D001','Data Migration','London','2016-05-10'),"
              + "('D002','Contracts Update','Glasgow','2016-05-10'))";
      st.execute(departmentFilling);

      String chiefOfficerFilling = "insert into CHIEF_OFFICER (FIRST_NAME,LAST_NAME,PROJECT) values (" + "('Tim','Cook','Mars'),"
          + "('Sundar','Pichai','Venus')," + "('Satya','Nadella','Iuppiter')," + "('Chuck','Robbins','Mercury'))";
      st.execute(chiefOfficerFilling);

      ODocument configDoc = OMigrationConfigManager.loadMigrationConfigFromFile(this.configPathJson);

      this.naiveStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, configDoc);

      /**
       *  Testing context information
       */

      assertEquals(12, context.getStatistics().totalNumberOfRecords);
      assertEquals(12, context.getStatistics().analyzedRecords);
      assertEquals(15, context.getStatistics().orientAddedVertices);
      assertEquals(15, context.getStatistics().orientAddedEdges);

      /**
       *  Testing built OrientDB
       */

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      // vertices check

      assertEquals(15, orientGraph.countClass("V"));
      assertEquals(5, orientGraph.countClass("Employee"));
      assertEquals(4, orientGraph.countClass("Project"));
      assertEquals(2, orientGraph.countClass("Department"));
      assertEquals(4, orientGraph.countClass("ChiefOfficer"));

      // edges check
      assertEquals(15, orientGraph.countClass("E"));
      assertEquals(5, orientGraph.countClass("WorksAt"));
      assertEquals(6, orientGraph.countClass("HasProject"));
      assertEquals(4, orientGraph.countClass("IsChiefForProject"));

      // vertex properties and connections check
      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "D001" };

      OVertex v = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Department", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("D001", v.getProperty("id"));
        assertEquals("Data Migration", v.getProperty("departmentName"));
        assertEquals("London", v.getProperty("location"));
        assertNull(v.getProperty("updatedOn"));
        edgesIt = v.getEdges(ODirection.IN, "WorksAt").iterator();
        assertEquals("Black", edgesIt.next().getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("McClanenei", edgesIt.next().getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("Durden", edgesIt.next().getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "D002";
      result = OGraphCommands.getVertices(orientGraph, "Department", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("D002", v.getProperty("id"));
        assertEquals("Contracts Update", v.getProperty("departmentName"));
        assertEquals("Glasgow", v.getProperty("location"));
        assertNull(v.getProperty("updatedOn"));
        edgesIt = v.getEdges(ODirection.IN, "WorksAt").iterator();
        assertEquals("McFly", edgesIt.next().getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("Anderson", edgesIt.next().getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      String[] employeeKeys = { "firstName", "lastName" };
      String[] employeeValues = { "Joe", "Black" };
      result = OGraphCommands.getVertices(orientGraph, "Employee", employeeKeys, employeeValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Joe", v.getProperty("firstName"));
        assertEquals("Black", v.getProperty("lastName"));
        assertEquals(20000, ((BigDecimal) v.getProperty("salary")).intValue());
        assertEquals("D001", v.getProperty("department"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAt").iterator();
        assertEquals("D001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasProject").iterator();
        OEdge currentSplittingEdge = edgesIt.next();
        assertEquals("Mars", currentSplittingEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals("T", currentSplittingEdge.getProperty("role"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      employeeValues[0] = "Thomas";
      employeeValues[1] = "Anderson";
      result = OGraphCommands.getVertices(orientGraph, "Employee", employeeKeys, employeeValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Thomas", v.getProperty("firstName"));
        assertEquals("Anderson", v.getProperty("lastName"));
        assertEquals(35000, ((BigDecimal) v.getProperty("salary")).intValue());
        assertEquals("D002", v.getProperty("department"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAt").iterator();
        assertEquals("D002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasProject").iterator();
        OEdge currentSplittingEdge = edgesIt.next();
        assertEquals("Venus", currentSplittingEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals("T", currentSplittingEdge.getProperty("role"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      employeeValues[0] = "Tyler";
      employeeValues[1] = "Durden";
      result = OGraphCommands.getVertices(orientGraph, "Employee", employeeKeys, employeeValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Tyler", v.getProperty("firstName"));
        assertEquals("Durden", v.getProperty("lastName"));
        assertEquals(35000, ((BigDecimal) v.getProperty("salary")).intValue());
        assertEquals("D001", v.getProperty("department"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAt").iterator();
        assertEquals("D001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasProject").iterator();
        OEdge currentSplittingEdge = edgesIt.next();
        assertEquals("Iuppiter", currentSplittingEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals("A", currentSplittingEdge.getProperty("role"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      employeeValues[0] = "John";
      employeeValues[1] = "McClanenei";
      result = OGraphCommands.getVertices(orientGraph, "Employee", employeeKeys, employeeValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("McClanenei", v.getProperty("lastName"));
        assertEquals(25000, ((BigDecimal) v.getProperty("salary")).intValue());
        assertEquals("D001", v.getProperty("department"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAt").iterator();
        assertEquals("D001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasProject").iterator();
        OEdge currentSplittingEdge = edgesIt.next();
        assertEquals("Venus", currentSplittingEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals("S", currentSplittingEdge.getProperty("role"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      employeeValues[0] = "Marty";
      employeeValues[1] = "McFly";
      result = OGraphCommands.getVertices(orientGraph, "Employee", employeeKeys, employeeValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Marty", v.getProperty("firstName"));
        assertEquals("McFly", v.getProperty("lastName"));
        assertEquals(40000, ((BigDecimal) v.getProperty("salary")).intValue());
        assertEquals("D002", v.getProperty("department"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAt").iterator();
        assertEquals("D002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasProject").iterator();
        OEdge currentSplittingEdge = edgesIt.next();
        assertEquals("Mars", currentSplittingEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals("M", currentSplittingEdge.getProperty("role"));
        currentSplittingEdge = edgesIt.next();
        assertEquals("Mercury", currentSplittingEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals("M", currentSplittingEdge.getProperty("role"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      String[] projectKeys = { "project" };
      String[] projectValues = { "Mars" };
      result = OGraphCommands.getVertices(orientGraph, "Project", projectKeys, projectValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Mars", v.getProperty("project"));
        assertEquals(12000, ((BigDecimal) v.getProperty("balance")).intValue());
        edgesIt = v.getEdges(ODirection.IN, "HasProject").iterator();
        OEdge currentSplittingEdge = edgesIt.next();
        assertEquals("Black", currentSplittingEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("T", currentSplittingEdge.getProperty("role"));
        currentSplittingEdge = edgesIt.next();
        assertEquals("McFly", currentSplittingEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("M", currentSplittingEdge.getProperty("role"));
        assertEquals(false, edgesIt.hasNext());

        edgesIt = v.getEdges(ODirection.IN, "IsChiefForProject").iterator();
        OEdge currentEdge = edgesIt.next();
        assertEquals("Tim", currentEdge.getVertex(ODirection.OUT).getProperty("firstName"));
        assertEquals("Cook", currentEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("Mars", currentEdge.getVertex(ODirection.OUT).getProperty("project"));
        assertEquals(0, currentEdge.getPropertyNames().size());
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      projectValues[0] = "Venus";
      result = OGraphCommands.getVertices(orientGraph, "Project", projectKeys, projectValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Venus", v.getProperty("project"));
        assertEquals(15000, ((BigDecimal) v.getProperty("balance")).intValue());
        edgesIt = v.getEdges(ODirection.IN, "HasProject").iterator();
        OEdge currentSplittingEdge = edgesIt.next();
        assertEquals("McClanenei", currentSplittingEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("S", currentSplittingEdge.getProperty("role"));
        currentSplittingEdge = edgesIt.next();
        assertEquals("Anderson", currentSplittingEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("T", currentSplittingEdge.getProperty("role"));
        assertEquals(false, edgesIt.hasNext());

        edgesIt = v.getEdges(ODirection.IN, "IsChiefForProject").iterator();
        OEdge currentEdge = edgesIt.next();
        assertEquals("Sundar", currentEdge.getVertex(ODirection.OUT).getProperty("firstName"));
        assertEquals("Pichai", currentEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("Venus", currentEdge.getVertex(ODirection.OUT).getProperty("project"));
        assertEquals(0, currentEdge.getPropertyNames().size());
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      projectValues[0] = "Iuppiter";
      result = OGraphCommands.getVertices(orientGraph, "Project", projectKeys, projectValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Iuppiter", v.getProperty("project"));
        assertEquals(20000, ((BigDecimal) v.getProperty("balance")).intValue());
        edgesIt = v.getEdges(ODirection.IN, "HasProject").iterator();
        OEdge currentSplittingEdge = edgesIt.next();
        assertEquals("Durden", currentSplittingEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("A", currentSplittingEdge.getProperty("role"));
        assertEquals(false, edgesIt.hasNext());

        edgesIt = v.getEdges(ODirection.IN, "IsChiefForProject").iterator();
        OEdge currentEdge = edgesIt.next();
        assertEquals("Satya", currentEdge.getVertex(ODirection.OUT).getProperty("firstName"));
        assertEquals("Nadella", currentEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("Iuppiter", currentEdge.getVertex(ODirection.OUT).getProperty("project"));
        assertEquals(0, currentEdge.getPropertyNames().size());
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      projectValues[0] = "Mercury";
      result = OGraphCommands.getVertices(orientGraph, "Project", projectKeys, projectValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Mercury", v.getProperty("project"));
        assertEquals(5000, ((BigDecimal) v.getProperty("balance")).intValue());
        edgesIt = v.getEdges(ODirection.IN, "HasProject").iterator();
        OEdge currentSplittingEdge = edgesIt.next();
        assertEquals("McFly", currentSplittingEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("M", currentSplittingEdge.getProperty("role"));
        assertEquals(false, edgesIt.hasNext());

        edgesIt = v.getEdges(ODirection.IN, "IsChiefForProject").iterator();
        OEdge currentEdge = edgesIt.next();
        assertEquals("Chuck", currentEdge.getVertex(ODirection.OUT).getProperty("firstName"));
        assertEquals("Robbins", currentEdge.getVertex(ODirection.OUT).getProperty("lastName"));
        assertEquals("Mercury", currentEdge.getVertex(ODirection.OUT).getProperty("project"));
        assertEquals(0, currentEdge.getPropertyNames().size());
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      String[] chiefOfficerKeys = { "firstName", "lastName" };
      String[] chiefOfficerValues = { "Tim", "Cook" };
      result = OGraphCommands.getVertices(orientGraph, "ChiefOfficer", chiefOfficerKeys, chiefOfficerValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Tim", v.getProperty("firstName"));
        assertEquals("Cook", v.getProperty("lastName"));
        assertEquals("Mars", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "IsChiefForProject").iterator();
        OEdge currentEdge = edgesIt.next();
        assertEquals("Mars", currentEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals(0, currentEdge.getPropertyNames().size());
        assertEquals(false, edgesIt.hasNext());

      } else {
        fail("Query fail!");
      }

      chiefOfficerValues[0] = "Sundar";
      chiefOfficerValues[1] = "Pichai";
      result = OGraphCommands.getVertices(orientGraph, "ChiefOfficer", chiefOfficerKeys, chiefOfficerValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Sundar", v.getProperty("firstName"));
        assertEquals("Pichai", v.getProperty("lastName"));
        assertEquals("Venus", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "IsChiefForProject").iterator();
        OEdge currentEdge = edgesIt.next();
        assertEquals("Venus", currentEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals(0, currentEdge.getPropertyNames().size());
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      chiefOfficerValues[0] = "Satya";
      chiefOfficerValues[1] = "Nadella";
      result = OGraphCommands.getVertices(orientGraph, "ChiefOfficer", chiefOfficerKeys, chiefOfficerValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Satya", v.getProperty("firstName"));
        assertEquals("Nadella", v.getProperty("lastName"));
        assertEquals("Iuppiter", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "IsChiefForProject").iterator();
        OEdge currentEdge = edgesIt.next();
        assertEquals("Iuppiter", currentEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals(0, currentEdge.getPropertyNames().size());
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      chiefOfficerValues[0] = "Chuck";
      chiefOfficerValues[1] = "Robbins";
      result = OGraphCommands.getVertices(orientGraph, "ChiefOfficer", chiefOfficerKeys, chiefOfficerValues);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("Chuck", v.getProperty("firstName"));
        assertEquals("Robbins", v.getProperty("lastName"));
        assertEquals("Mercury", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "IsChiefForProject").iterator();
        OEdge currentEdge = edgesIt.next();
        assertEquals("Mercury", currentEdge.getVertex(ODirection.IN).getProperty("project"));
        assertEquals(0, currentEdge.getPropertyNames().size());
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

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
      if (orientGraph != null) {
        orientGraph.close();
      }

    }
  }

}
