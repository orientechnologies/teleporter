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

package com.orientechnologies.teleporter.test.rdbms.inheritance.hibernate;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.orientechnologies.teleporter.util.OFileManager;
import com.orientechnologies.teleporter.util.OGraphCommands;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class HibernateImportTest {

  private OTeleporterContext context;
  private ODBMSNaiveStrategy importStrategy;
  private ODBQueryEngine     dbQueryEngine;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private String dbName = "testOrientDB";
  private String outParentDirectory = "embedded:target/";
  private String outOrientGraphUri = this.outParentDirectory + this.dbName;
  private OSourceDatabaseInfo sourceDBInfo;

  private final static String XML_TABLE_PER_CLASS          = "src/test/resources/inheritance/hibernate/tablePerClassHierarchyImportTest.xml";
  private final static String XML_TABLE_PER_SUBCLASS1      = "src/test/resources/inheritance/hibernate/tablePerSubclassImportTest1.xml";
  private final static String XML_TABLE_PER_SUBCLASS2      = "src/test/resources/inheritance/hibernate/tablePerSubclassImportTest2.xml";
  private final static String XML_TABLE_PER_CONCRETE_CLASS = "src/test/resources/inheritance/hibernate/tablePerConcreteClassImportTest.xml";


  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance(this.outParentDirectory);
    this.context.initOrientDBInstance(outOrientGraphUri);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setMessageHandler(new OTeleporterMessageHandler(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.importStrategy = new ODBMSNaiveStrategy("embedded", this.outParentDirectory, this.dbName);
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

  //@Test
  /*
   * Import from tables with "table-per-hierarchy" inheritance strategy.
   * Relationships both to simple table and table in a hierarchical bag ("table-per-hierarchy" bag).
   */ public void test1() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, TYPE varchar(256), NAME varchar(256), PROJECT varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
          + " TYPE varchar(256), NAME varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), "
          + "primary key (id), foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);

      // Records Inserting

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','Italy')," + "('R002','Milan','Italy'))";
      st.execute(residenceFilling);

      String managerFilling =
          "insert into manager (ID,TYPE,NAME,PROJECT) values (" + "('M001','prj_mgr','Bill Right','New World'))";
      st.execute(managerFilling);

      String employeeFilling =
          "insert into EMPLOYEE (ID,TYPE,NAME,SALARY,BONUS,PAY_PER_HOUR,CONTRACT_DURATION,RESIDENCE,MANAGER) values ("
              + "('E001','emp','John Black',NULL,NULL,NULL,NULL,'R001',NULL),"
              + "('E002','reg_emp','Andrew Brown','1000.00','10',NULL,NULL,'R001','M001'),"
              + "('E003','cont_emp','Jack Johnson',NULL,NULL,'50.00','6','R002',NULL))";
      st.execute(employeeFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "hibernate", HibernateImportTest.XML_TABLE_PER_CLASS, "java",
              null, null, null);

      /*
       *  Testing context information
       */

      assertEquals(6, context.getStatistics().totalNumberOfRecords);
      assertEquals(6, context.getStatistics().analyzedRecords);
      assertEquals(6, context.getStatistics().orientAddedVertices);
      assertEquals(4, context.getStatistics().orientAddedEdges);


      /*
       * Test OrientDB Schema
       */

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      OClass employeeVertexType = orientGraph.getClass("Employee");
      OClass regularEmployeeVertexType = orientGraph.getClass("RegularEmployee");
      OClass contractEmployeeVertexType = orientGraph.getClass("ContractEmployee");

      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      OClass employeeSuperclass = employeeVertexType.getSuperClass();
      OClass regularEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();
      OClass contractEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();

      assertNotNull(employeeSuperclass);
      assertEquals("V", employeeSuperclass.getName());
      assertNotNull(regularEmployeeSuperclass);
      assertEquals("Employee", regularEmployeeSuperclass.getName());
      assertNotNull(contractEmployeeSuperclass);
      assertEquals("Employee", contractEmployeeSuperclass.getName());

      OClass managerVertexType = orientGraph.getClass("Manager");
      OClass projectManagerVertexType = orientGraph.getClass("ProjectManager");

      assertNotNull(managerVertexType);
      assertNotNull(projectManagerVertexType);

      OClass managerSuperclass = managerVertexType.getSuperClass();
      OClass projectManagerSuperclass = projectManagerVertexType.getSuperClass();

      assertNotNull(managerSuperclass);
      assertEquals("V", managerSuperclass.getName());
      assertNotNull(projectManagerSuperclass);
      assertEquals("Manager", projectManagerSuperclass.getName());


      /*
       *  Testing built OrientDB
       */

      // vertices check

      assertEquals(6, orientGraph.countClass("V"));
      assertEquals(3, orientGraph.countClass("Employee"));
      assertEquals(1, orientGraph.countClass("RegularEmployee"));
      assertEquals(1, orientGraph.countClass("ContractEmployee"));
      assertEquals(2, orientGraph.countClass("Residence"));
      assertEquals(1, orientGraph.countClass("Manager"));
      assertEquals(1, orientGraph.countClass("ProjectManager"));

      // edges check

      assertEquals(4, orientGraph.countClass("E"));
      assertEquals(3, orientGraph.countClass("HasResidence"));
      assertEquals(1, orientGraph.countClass("HasManager"));

      // vertex properties and connections check

      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "E001" };

      OVertex v = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E002";
      result = OGraphCommands.getVertices(orientGraph, "RegularEmployee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        Object o = v.getProperty("salary");
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("10", v.getProperty("bonus").toString());
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasManager").iterator();
        assertEquals("M001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E003";
      result = OGraphCommands.getVertices(orientGraph, "ContractEmployee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertEquals("50.00", v.getProperty("payPerHour").toString());
        assertEquals("6", v.getProperty("contractDuration").toString());

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "R001";
      result = OGraphCommands.getVertices(orientGraph, "Residence", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("R001", v.getProperty("id"));
        assertEquals("Rome", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(ODirection.IN, "HasResidence").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals("E001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "R002";
      result = OGraphCommands.getVertices(orientGraph, "Residence", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("R002", v.getProperty("id"));
        assertEquals("Milan", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(ODirection.IN, "HasResidence").iterator();
        assertEquals("E003", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "M001";
      result = OGraphCommands.getVertices(orientGraph, "Manager", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
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
    }
  }

  //@Test
  /*
   * Import from tables with "table-per-type" inheritance strategy.
   * Relationships both to simple table and table in a hierarchical bag ("table-per-type" bag).
   */ public void test2() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String projectManagerTableBuilding = "create memory table PROJECT_MANAGER(EID varchar(256) not null, PROJECT varchar(256), primary key (EID), foreign key (EID) references MANAGER(ID))";
      st.execute(projectManagerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
          + " NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID), "
          + "foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      // Records Inserting

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','Italy')," + "('R002','Milan','Italy'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values (" + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling = "insert into PROJECT_MANAGER (EID,PROJECT) values (" + "('M001','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values (" + "('E001','John Black','R001',NULL),"
          + "('E002','Andrew Brown','R001','M001')," + "('E003','Jack Johnson','R002',NULL))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values (" + "('E002','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling =
          "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values (" + "('E003','50.00','6'))";
      st.execute(contractEmployeeFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "hibernate", HibernateImportTest.XML_TABLE_PER_SUBCLASS1,
              "java", null, null, null);

      /*
       *  Testing context information
       */

      assertEquals(9, context.getStatistics().totalNumberOfRecords);
      assertEquals(9, context.getStatistics().analyzedRecords);
      assertEquals(6, context.getStatistics().orientAddedVertices);
      assertEquals(4, context.getStatistics().orientAddedEdges);


      /*
       * Test OrientDB Schema
       */

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      OClass employeeVertexType = orientGraph.getClass("Employee");
      OClass regularEmployeeVertexType = orientGraph.getClass("RegularEmployee");
      OClass contractEmployeeVertexType = orientGraph.getClass("ContractEmployee");

      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      OClass employeeSuperclass = employeeVertexType.getSuperClass();
      OClass regularEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();
      OClass contractEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();

      assertNotNull(employeeSuperclass);
      assertEquals("V", employeeSuperclass.getName());
      assertNotNull(regularEmployeeSuperclass);
      assertEquals("Employee", regularEmployeeSuperclass.getName());
      assertNotNull(contractEmployeeSuperclass);
      assertEquals("Employee", contractEmployeeSuperclass.getName());

      OClass managerVertexType = orientGraph.getClass("Manager");
      OClass projectManagerVertexType = orientGraph.getClass("ProjectManager");

      assertNotNull(managerVertexType);
      assertNotNull(projectManagerVertexType);

      OClass managerSuperclass = managerVertexType.getSuperClass();
      OClass projectManagerSuperclass = projectManagerVertexType.getSuperClass();

      assertNotNull(managerSuperclass);
      assertEquals("V", managerSuperclass.getName());
      assertNotNull(projectManagerSuperclass);
      assertEquals("Manager", projectManagerSuperclass.getName());


      /*
       *  Testing built OrientDB
       */

      // vertices check

//      int count = 0;
//      for(OVertex v : orientGraph.command(this.getVerticesQuery)) {
//        assertNotNull(v.getIdentity());
//        count++;
//      }
      assertEquals(6, orientGraph.countClass("V"));

//      count = 0;
//      for(OVertex v : orientGraph.command(this.getElementsFromClassQuery, "Employee")) {
//        assertNotNull(v.getIdentity());
//        count++;
//      }
      assertEquals(3, orientGraph.countClass("Employee"));

//      count = 0;
//      for(OVertex v : orientGraph.command(this.getElementsFromClassQuery, "RegularEmployee")) {
//        assertNotNull(v.getIdentity());
//        count++;
//      }
      assertEquals(1, orientGraph.countClass("RegularEmployee"));

//      count = 0;
//      for(OVertex v : orientGraph.command(this.getElementsFromClassQuery, "ContractEmployee")) {
//        assertNotNull(v.getIdentity());
//        count++;
//      }
      assertEquals(1, orientGraph.countClass("ContractEmployee"));

//      count = 0;
//      for(OVertex v : orientGraph.command(this.getElementsFromClassQuery, "Residence")) {
//        assertNotNull(v.getIdentity());
//        count++;
//      }
      assertEquals(2, orientGraph.countClass("Residence"));

//      count = 0;
//      for(OVertex v : orientGraph.command(this.getElementsFromClassQuery, "Manager")) {
//        assertNotNull(v.getIdentity());
//        count++;
//      }
      assertEquals(1, orientGraph.countClass("Manager"));

//      count = 0;
//      for(OVertex v : orientGraph.command(this.getElementsFromClassQuery, "ProjectManager")) {
//        assertNotNull(v.getIdentity());
//        count++;
//      }
      assertEquals(1, orientGraph.countClass("ProjectManager"));

      // edges check
//      count = 0;
//      for (OEdge  e : orientGraph.command(this.getEdgesQuery)) {
//        assertNotNull(e.getIdentity());
//        count++;
//      }
      assertEquals(4, orientGraph.countClass("E"));

//      count = 0;
//      for (OEdge  e : orientGraph.command(this.getElementsFromClassQuery, "HasResidence")) {
//        assertNotNull(e.getIdentity());
//        count++;
//      }
      assertEquals(3, orientGraph.countClass("HasResidence"));

//      count = 0;
//      for (OEdge  e : orientGraph.command(this.getElementsFromClassQuery, "HasManager")) {
//        assertNotNull(e.getIdentity());
//        count++;
//      }
      assertEquals(1, orientGraph.countClass("HasManager"));

      // vertex properties and connections check

      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "E001" };

      OVertex v = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E002";
      result = OGraphCommands.getVertices(orientGraph, "RegularEmployee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("10", v.getProperty("bonus").toString());
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasManager").iterator();
        assertEquals("M001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E003";
      result = OGraphCommands.getVertices(orientGraph, "ContractEmployee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertEquals("50.00", v.getProperty("payPerHour").toString());
        assertEquals("6", v.getProperty("contractDuration").toString());

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "R001";
      result = OGraphCommands.getVertices(orientGraph, "Residence", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("R001", v.getProperty("id"));
        assertEquals("Rome", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(ODirection.IN, "HasResidence").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals("E001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "R002";
      result = OGraphCommands.getVertices(orientGraph, "Residence", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("R002", v.getProperty("id"));
        assertEquals("Milan", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(ODirection.IN, "HasResidence").iterator();
        assertEquals("E003", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "M001";
      result = OGraphCommands.getVertices(orientGraph, "Manager", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
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
    }
  }

  //@Test
  /*
   * Import from tables with "table-per-type" inheritance strategy.
   * Relationships both to simple table and table in a hierarchical bag ("table-per-type" bag).
   */ public void test3() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String projectManagerTableBuilding = "create memory table PROJECT_MANAGER(EID varchar(256) not null, PROJECT varchar(256), primary key (EID), foreign key (EID) references MANAGER(ID))";
      st.execute(projectManagerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
          + " NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID), "
          + "foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      // Records Inserting

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','Italy')," + "('R002','Milan','Italy'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values (" + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling = "insert into PROJECT_MANAGER (EID,PROJECT) values (" + "('M001','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values (" + "('E001','John Black','R001',NULL),"
          + "('E002','Andrew Brown','R001','M001')," + "('E003','Jack Johnson','R002',NULL))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values (" + "('E002','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling =
          "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values (" + "('E003','50.00','6'))";
      st.execute(contractEmployeeFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "hibernate", HibernateImportTest.XML_TABLE_PER_SUBCLASS2,
              "java", null, null, null);

      /*
       *  Testing context information
       */

      assertEquals(9, context.getStatistics().totalNumberOfRecords);
      assertEquals(9, context.getStatistics().analyzedRecords);
      assertEquals(6, context.getStatistics().orientAddedVertices);
      assertEquals(4, context.getStatistics().orientAddedEdges);


      /*
       * Test OrientDB Schema
       */

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      OClass employeeVertexType = orientGraph.getClass("Employee");
      OClass regularEmployeeVertexType = orientGraph.getClass("RegularEmployee");
      OClass contractEmployeeVertexType = orientGraph.getClass("ContractEmployee");

      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      OClass employeeSuperclass = employeeVertexType.getSuperClass();
      OClass regularEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();
      OClass contractEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();

      assertNotNull(employeeSuperclass);
      assertEquals("V", employeeSuperclass.getName());
      assertNotNull(regularEmployeeSuperclass);
      assertEquals("Employee", regularEmployeeSuperclass.getName());
      assertNotNull(contractEmployeeSuperclass);
      assertEquals("Employee", contractEmployeeSuperclass.getName());

      OClass managerVertexType = orientGraph.getClass("Manager");
      OClass projectManagerVertexType = orientGraph.getClass("ProjectManager");

      assertNotNull(managerVertexType);
      assertNotNull(projectManagerVertexType);

      OClass managerSuperclass = managerVertexType.getSuperClass();
      OClass projectManagerSuperclass = projectManagerVertexType.getSuperClass();

      assertNotNull(managerSuperclass);
      assertEquals("V", managerSuperclass.getName());
      assertNotNull(projectManagerSuperclass);
      assertEquals("Manager", projectManagerSuperclass.getName());


      /*
       *  Testing built OrientDB
       */

      // vertices check

      assertEquals(6, orientGraph.countClass("V"));
      assertEquals(3, orientGraph.countClass("Employee"));
      assertEquals(1, orientGraph.countClass("RegularEmployee"));
      assertEquals(1, orientGraph.countClass("ContractEmployee"));
      assertEquals(2, orientGraph.countClass("Residence"));
      assertEquals(1, orientGraph.countClass("Manager"));
      assertEquals(1, orientGraph.countClass("ProjectManager"));

      // edges check

      assertEquals(4, orientGraph.countClass("E"));
      assertEquals(3, orientGraph.countClass("HasResidence"));
      assertEquals(1, orientGraph.countClass("HasManager"));

      // vertex properties and connections check

      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "E001" };

      OVertex v = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E002";
      result = OGraphCommands.getVertices(orientGraph, "RegularEmployee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("10", v.getProperty("bonus").toString());
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasManager").iterator();
        assertEquals("M001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E003";
      result = OGraphCommands.getVertices(orientGraph, "ContractEmployee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertEquals("50.00", v.getProperty("payPerHour").toString());
        assertEquals("6", v.getProperty("contractDuration").toString());

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "R001";
      result = OGraphCommands.getVertices(orientGraph, "Residence", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("R001", v.getProperty("id"));
        assertEquals("Rome", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(ODirection.IN, "HasResidence").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals("E001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "R002";
      result = OGraphCommands.getVertices(orientGraph, "Residence", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("R002", v.getProperty("id"));
        assertEquals("Milan", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(ODirection.IN, "HasResidence").iterator();
        assertEquals("E003", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "M001";
      result = OGraphCommands.getVertices(orientGraph, "Manager", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
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
    }
  }

  //@Test
  /*
   * Import from tables with "table-per-concrete-type" inheritance strategy.
   * Relationships both to simple table and table in a hierarchical bag ("table-per-concrete-type" bag).
   */ public void test4() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String projectManagerTableBuilding = "create memory table PROJECT_MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT varchar(256), primary key (ID))";
      st.execute(projectManagerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
          + " NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID), "
          + "foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (ID varchar(256) not null, "
          + "NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256),"
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (ID varchar(256) not null, "
          + "NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256),"
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (ID))";
      st.execute(contractEmployeeTableBuilding);

      // Records Inserting

      String residenceFilling =
          "insert into RESIDENCE (ID,CITY,COUNTRY) values (" + "('R001','Rome','Italy')," + "('R002','Milan','Italy'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values (" + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling =
          "insert into PROJECT_MANAGER (ID,NAME,PROJECT) values (" + "('M001','Bill Right','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values (" + "('E001','John Black','R001',NULL),"
          + "('E002','Andrew Brown','R001','M001')," + "('E003','Jack Johnson','R002',NULL))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,SALARY,BONUS) values ("
          + "('E002','Andrew Brown','R001','M001','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling =
          "insert into CONTRACT_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,PAY_PER_HOUR,CONTRACT_DURATION) values ("
              + "('E003','Jack Johnson','R002',NULL,'50.00','6'))";
      st.execute(contractEmployeeFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "hibernate", HibernateImportTest.XML_TABLE_PER_CONCRETE_CLASS,
              "java", null, null, null);

      /*
       *  Testing context information
       */

      assertEquals(9, context.getStatistics().totalNumberOfRecords);
      assertEquals(9, context.getStatistics().analyzedRecords);
      assertEquals(6, context.getStatistics().orientAddedVertices);
      assertEquals(4, context.getStatistics().orientAddedEdges);


      /*
       * Test OrientDB Schema
       */

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      OClass employeeVertexType = orientGraph.getClass("Employee");
      OClass regularEmployeeVertexType = orientGraph.getClass("RegularEmployee");
      OClass contractEmployeeVertexType = orientGraph.getClass("ContractEmployee");

      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      OClass employeeSuperclass = employeeVertexType.getSuperClass();
      OClass regularEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();
      OClass contractEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();

      assertNotNull(employeeSuperclass);
      assertEquals("V", employeeSuperclass.getName());
      assertNotNull(regularEmployeeSuperclass);
      assertEquals("Employee", regularEmployeeSuperclass.getName());
      assertNotNull(contractEmployeeSuperclass);
      assertEquals("Employee", contractEmployeeSuperclass.getName());

      OClass managerVertexType = orientGraph.getClass("Manager");
      OClass projectManagerVertexType = orientGraph.getClass("ProjectManager");

      assertNotNull(managerVertexType);
      assertNotNull(projectManagerVertexType);

      OClass managerSuperclass = managerVertexType.getSuperClass();
      OClass projectManagerSuperclass = projectManagerVertexType.getSuperClass();

      assertNotNull(managerSuperclass);
      assertEquals("V", managerSuperclass.getName());
      assertNotNull(projectManagerSuperclass);
      assertEquals("Manager", projectManagerSuperclass.getName());


      /*
       *  Testing built OrientDB
       */

      // vertices check

      assertEquals(6, orientGraph.countClass("V"));
      assertEquals(3, orientGraph.countClass("Employee"));
      assertEquals(1, orientGraph.countClass("RegularEmployee"));
      assertEquals(1, orientGraph.countClass("ContractEmployee"));
      assertEquals(2, orientGraph.countClass("Residence"));
      assertEquals(1, orientGraph.countClass("Manager"));
      assertEquals(1, orientGraph.countClass("ProjectManager"));

      // edges check

      assertEquals(4, orientGraph.countClass("E"));
      assertEquals(3, orientGraph.countClass("HasResidence"));
      assertEquals(1, orientGraph.countClass("HasManager"));

      // vertex properties and connections check

      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "E001" };

      OVertex v = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E002";
      result = OGraphCommands.getVertices(orientGraph, "RegularEmployee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("10", v.getProperty("bonus").toString());
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasManager").iterator();
        assertEquals("M001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E003";
      result = OGraphCommands.getVertices(orientGraph, "ContractEmployee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertEquals("50.00", v.getProperty("payPerHour").toString());
        assertEquals("6", v.getProperty("contractDuration").toString());

        edgesIt = v.getEdges(ODirection.OUT, "HasResidence").iterator();
        assertEquals("R002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "R001";
      result = OGraphCommands.getVertices(orientGraph, "Residence", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("R001", v.getProperty("id"));
        assertEquals("Rome", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(ODirection.IN, "HasResidence").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals("E001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "R002";
      result = OGraphCommands.getVertices(orientGraph, "Residence", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("R002", v.getProperty("id"));
        assertEquals("Milan", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(ODirection.IN, "HasResidence").iterator();
        assertEquals("E003", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "M001";
      result = OGraphCommands.getVertices(orientGraph, "Manager", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
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
    }
  }

}