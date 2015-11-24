/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.teleporter.test.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.ONaiveImportStrategy;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OFilterTableImportingTestCase {

  private OTeleporterContext context;
  private ONaiveImportStrategy importStrategy;
  private String outOrientGraphUri;

  private final static String XML_TABLE_PER_CLASS = "src/main/resources/inheritance/hibernate/tablePerClassHierarchyImportTest.xml";
  private final static String XML_TABLE_PER_SUBCLASS1 = "src/main/resources/inheritance/hibernate/tablePerSubclassImportTest1.xml";
  private final static String XML_TABLE_PER_SUBCLASS2 = "src/main/resources/inheritance/hibernate/tablePerSubclassImportTest2.xml";
  private final static String XML_TABLE_PER_CONCRETE_CLASS = "src/main/resources/inheritance/hibernate/tablePerConcreteClassImportTest.xml";



  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.context.setQueryQuoteType("\"");
    this.importStrategy = new ONaiveImportStrategy();
    this.outOrientGraphUri = "memory:testOrientDB";
  }

  @Test
  /*
   * Filtering out a table through include-tables (without inheritance).
   */
  public void test1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), "
          + "primary key (id), foreign key (country) references country(id))";
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), SALARY decimal(10,2), RESIDENCE varchar(256), MANAGER varchar(256), "
          + "primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);


      // Records Inserting

      String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values ("
          + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','C001'),"
          + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME,PROJECT) values ("
          + "('M001','Bill Right','New World'))";
      st.execute(managerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,SALARY,RESIDENCE,MANAGER) values ("
          + "('E001','John Black',1500.00,'R001',null),"
          + "('E002','Andrew Brown','1000.00','R001','M001'),"
          + "('E003','Jack Johnson',2000.00,'R002',null))";
      st.execute(employeeFilling);

      List<String> includedTables = new ArrayList<String>();
      includedTables.add("COUNTRY");
      includedTables.add("MANAGER");
      includedTables.add("EMPLOYEE");

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", includedTables, null, context);


      /*
       *  Testing context information
       */

      assertEquals(5, context.getStatistics().totalNumberOfRecords);
      assertEquals(5, context.getStatistics().analyzedRecords);
      assertEquals(5, context.getStatistics().orientAddedVertices);


      /*
       * Test OrientDB Schema
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType employeeVertexType = orientGraph.getVertexType("Employee");
      OrientVertexType countryVertexType = orientGraph.getVertexType("Country");
      OrientVertexType managerVertexType = orientGraph.getVertexType("Manager");
      OrientVertexType regularEmployeeVertexType = orientGraph.getVertexType("RegularEmployee");
      OrientVertexType contractEmployeeVertexType = orientGraph.getVertexType("ContractEmployee");
      OrientVertexType projectManagerVertexType = orientGraph.getVertexType("ProjectManager");
      OrientVertexType residenceVertexType = orientGraph.getVertexType("Residence");


      assertNotNull(employeeVertexType);
      assertNotNull(countryVertexType);
      assertNotNull(managerVertexType);
      assertNull(regularEmployeeVertexType);
      assertNull(contractEmployeeVertexType);
      assertNull(projectManagerVertexType);
      assertNull(residenceVertexType);


      /*
       *  Testing built OrientDB
       */

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
//      assertEquals(5, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Manager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Country")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasManager")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);


      // vertex properties and connections check

      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"E001"};

      OrientVertex v = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1500.00", v.getProperty("salary").toString());
        assertNull(v.getProperty("manager"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E002";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("M001", v.getProperty("manager"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasManager").iterator();
        assertEquals("M001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E003";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertEquals("2000.00", v.getProperty("salary").toString());
        assertNull(v.getProperty("manager"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "M001";
      iterator = orientGraph.getVertices("Manager", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }
      
      values[0] = "C001";
      iterator = orientGraph.getVertices("Country", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("C001", v.getProperty("id"));
        assertEquals("Italy", v.getProperty("name"));
        assertEquals("Europe", v.getProperty("continent"));

        edgesIt = v.getEdges(Direction.IN).iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      
    } catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }


  @Test
  /*
   * Filtering out a table through exclude-tables (without inheritance).
   */
  public void test2() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), "
          + "primary key (ID), foreign key (COUNTRY) references COUNTRY(ID))";
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), SALARY decimal(10,2), RESIDENCE varchar(256), MANAGER varchar(256), "
          + "primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);


      // Records Inserting

      String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values ("
          + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','C001'),"
          + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME,PROJECT) values ("
          + "('M001','Bill Right','New World'))";
      st.execute(managerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,SALARY,RESIDENCE,MANAGER) values ("
          + "('E001','John Black',1500.00,'R001',null),"
          + "('E002','Andrew Brown','1000.00','R001','M001'),"
          + "('E003','Jack Johnson',2000.00,'R002',null))";
      st.execute(employeeFilling);

      List<String> excludedTables = new ArrayList<String>();
      excludedTables.add("RESIDENCE");

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, excludedTables, context);


      /*
       *  Testing context information
       */

      assertEquals(5, context.getStatistics().totalNumberOfRecords);
      assertEquals(5, context.getStatistics().analyzedRecords);
      assertEquals(5, context.getStatistics().orientAddedVertices);


      /*
       * Test OrientDB Schema
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType employeeVertexType = orientGraph.getVertexType("Employee");
      OrientVertexType countryVertexType = orientGraph.getVertexType("Country");
      OrientVertexType managerVertexType = orientGraph.getVertexType("Manager");
      OrientVertexType regularEmployeeVertexType = orientGraph.getVertexType("RegularEmployee");
      OrientVertexType contractEmployeeVertexType = orientGraph.getVertexType("ContractEmployee");
      OrientVertexType projectManagerVertexType = orientGraph.getVertexType("ProjectManager");
      OrientVertexType residenceVertexType = orientGraph.getVertexType("Residence");


      assertNotNull(employeeVertexType);
      assertNotNull(countryVertexType);
      assertNotNull(managerVertexType);
      assertNull(regularEmployeeVertexType);
      assertNull(contractEmployeeVertexType);
      assertNull(projectManagerVertexType);
      assertNull(residenceVertexType);


      /*
       *  Testing built OrientDB
       */

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(5, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Manager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Country")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasManager")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);


      // vertex properties and connections check

      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"E001"};

      OrientVertex v = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1500.00", v.getProperty("salary").toString());
        assertNull(v.getProperty("manager"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E002";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("M001", v.getProperty("manager"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasManager").iterator();
        assertEquals("M001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E003";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertEquals("2000.00", v.getProperty("salary").toString());
        assertNull(v.getProperty("manager"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "M001";
      iterator = orientGraph.getVertices("Manager", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "C001";
      iterator = orientGraph.getVertices("Country", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("C001", v.getProperty("id"));
        assertEquals("Italy", v.getProperty("name"));
        assertEquals("Europe", v.getProperty("continent"));

        edgesIt = v.getEdges(Direction.IN).iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      
    } catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }


  @Test
  /*
   * Filtering out a table through include-tables (with Table per Hierarchy inheritance).
   */
  public void test3() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), "
          + "primary key (ID), foreign key (COUNTRY) references COUNTRY(ID))";
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, TYPE varchar(256), NAME varchar(256), PROJECT varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " TYPE varchar(256), NAME varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), "
          + "primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);


      // Records Inserting

      String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values ("
          + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','C001'),"
          + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,TYPE,NAME,PROJECT) values ("
          + "('M001','prj_mgr','Bill Right','New World'))";
      st.execute(managerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,TYPE,NAME,SALARY,BONUS,PAY_PER_HOUR,CONTRACT_DURATION,RESIDENCE,MANAGER) values ("
          + "('E001','emp','John Black',null,null,null,null,'R001',null),"
          + "('E002','reg_emp','Andrew Brown','1000.00','10',null,null,'R001','M001'),"
          + "('E003','cont_emp','Jack Johnson',null,null,'50.00','6','R002',null))";
      st.execute(employeeFilling);

      List<String> includedTables = new ArrayList<String>();
      includedTables.add("COUNTRY");
      includedTables.add("MANAGER");
      includedTables.add("EMPLOYEE");

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", OFilterTableImportingTestCase.XML_TABLE_PER_CLASS, "java", includedTables, null, context);

      /*
       *  Testing context information
       */

      assertEquals(5, context.getStatistics().totalNumberOfRecords);
      assertEquals(5, context.getStatistics().analyzedRecords);
      assertEquals(5, context.getStatistics().orientAddedVertices);


      /*
       * Test OrientDB Schema
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType employeeVertexType = orientGraph.getVertexType("Employee");
      OrientVertexType regularEmployeeVertexType = orientGraph.getVertexType("RegularEmployee");
      OrientVertexType contractEmployeeVertexType = orientGraph.getVertexType("ContractEmployee");

      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      OrientVertexType employeeSuperclass = employeeVertexType.getSuperClass();
      OrientVertexType regularEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();
      OrientVertexType contractEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();

      assertNotNull(employeeSuperclass);
      assertEquals("V", employeeSuperclass.getName());
      assertNotNull(regularEmployeeSuperclass);
      assertEquals("Employee", regularEmployeeSuperclass.getName());
      assertNotNull(contractEmployeeSuperclass);
      assertEquals("Employee", contractEmployeeSuperclass.getName());

      OrientVertexType managerVertexType = orientGraph.getVertexType("Manager");
      OrientVertexType projectManagerVertexType = orientGraph.getVertexType("ProjectManager");

      assertNotNull(managerVertexType);
      assertNotNull(projectManagerVertexType);

      OrientVertexType managerSuperclass = managerVertexType.getSuperClass();
      OrientVertexType projectManagerSuperclass = projectManagerVertexType.getSuperClass();

      assertNotNull(managerSuperclass);
      assertEquals("V", managerSuperclass.getName());
      assertNotNull(projectManagerSuperclass);
      assertEquals("Manager", projectManagerSuperclass.getName());


      /*
       *  Testing built OrientDB
       */

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(5, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("RegularEmployee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("ContractEmployee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Manager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("ProjectManager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasManager")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);


      // vertex properties and connections check

      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"E001"};

      OrientVertex v = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E002";
      iterator = orientGraph.getVertices("RegularEmployee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("10", v.getProperty("bonus").toString());
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasManager").iterator();
        assertEquals("M001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E003";
      iterator = orientGraph.getVertices("ContractEmployee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertEquals("50.00", v.getProperty("payPerHour").toString());
        assertEquals("6", v.getProperty("contractDuration").toString());

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "M001";
      iterator = orientGraph.getVertices("Manager", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }
      
      values[0] = "C001";
      iterator = orientGraph.getVertices("Country", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("C001", v.getProperty("id"));
        assertEquals("Italy", v.getProperty("name"));
        assertEquals("Europe", v.getProperty("continent"));

        edgesIt = v.getEdges(Direction.IN).iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


    }catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }

  @Test
  /*
   * Filtering out a table through exclude-tables (with Table per Type inheritance).
   */
  public void test4() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String projectManagerTableBuilding = "create memory table PROJECT_MANAGER(EID varchar(256) not null, PROJECT varchar(256), primary key (EID), foreign key (EID) references MANAGER(ID))";
      st.execute(projectManagerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID), "
          + "foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references manager(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);


      // Records Inserting

      String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values ("
          + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','C001'),"
          + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values ("
          + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling = "insert into PROJECT_MANAGER (EID,PROJECT) values ("
          + "('M001','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values ("
          + "('E001','John Black','R001',null),"
          + "('E002','Andrew Brown','R001','M001'),"
          + "('E003','Jack Johnson','R002',null))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values ("
          + "('E002','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling = "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values ("
          + "('E003','50.00','6'))";
      st.execute(contractEmployeeFilling);

      List<String> includedTables = new ArrayList<String>();
      includedTables.add("COUNTRY");
      includedTables.add("MANAGER");
      includedTables.add("PROJECT_MANAGER");
      includedTables.add("EMPLOYEE");
      includedTables.add("REGULAR_EMPLOYEE");
      includedTables.add("CONTRACT_EMPLOYEE");

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", OFilterTableImportingTestCase.XML_TABLE_PER_SUBCLASS1, "java", includedTables, null, context);

      /*
       *  Testing context information
       */

      assertEquals(8, context.getStatistics().totalNumberOfRecords);
      assertEquals(8, context.getStatistics().analyzedRecords);
      assertEquals(5, context.getStatistics().orientAddedVertices);


      /*
       * Test OrientDB Schema
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType employeeVertexType = orientGraph.getVertexType("Employee");
      OrientVertexType regularEmployeeVertexType = orientGraph.getVertexType("RegularEmployee");
      OrientVertexType contractEmployeeVertexType = orientGraph.getVertexType("ContractEmployee");

      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      OrientVertexType employeeSuperclass = employeeVertexType.getSuperClass();
      OrientVertexType regularEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();
      OrientVertexType contractEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();

      assertNotNull(employeeSuperclass);
      assertEquals("V", employeeSuperclass.getName());
      assertNotNull(regularEmployeeSuperclass);
      assertEquals("Employee", regularEmployeeSuperclass.getName());
      assertNotNull(contractEmployeeSuperclass);
      assertEquals("Employee", contractEmployeeSuperclass.getName());

      OrientVertexType managerVertexType = orientGraph.getVertexType("Manager");
      OrientVertexType projectManagerVertexType = orientGraph.getVertexType("ProjectManager");

      assertNotNull(managerVertexType);
      assertNotNull(projectManagerVertexType);

      OrientVertexType managerSuperclass = managerVertexType.getSuperClass();
      OrientVertexType projectManagerSuperclass = projectManagerVertexType.getSuperClass();

      assertNotNull(managerSuperclass);
      assertEquals("V", managerSuperclass.getName());
      assertNotNull(projectManagerSuperclass);
      assertEquals("Manager", projectManagerSuperclass.getName());


      /*
       *  Testing built OrientDB
       */

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(5, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("RegularEmployee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("ContractEmployee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Manager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("ProjectManager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasManager")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);


      // vertex properties and connections check

      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"E001"};

      OrientVertex v = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E002";
      iterator = orientGraph.getVertices("RegularEmployee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("10", v.getProperty("bonus").toString());
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasManager").iterator();
        assertEquals("M001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E003";
      iterator = orientGraph.getVertices("ContractEmployee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertEquals("50.00", v.getProperty("payPerHour").toString());
        assertEquals("6", v.getProperty("contractDuration").toString());

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "M001";
      iterator = orientGraph.getVertices("Manager", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }
      
      values[0] = "C001";
      iterator = orientGraph.getVertices("Country", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("C001", v.getProperty("id"));
        assertEquals("Italy", v.getProperty("name"));
        assertEquals("Europe", v.getProperty("continent"));

        edgesIt = v.getEdges(Direction.IN).iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


    } catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }

  @Test
  /*
   * Filtering out a table through exclude-tables (with Table per Type inheritance).
   */
  public void test5() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String projectManagerTableBuilding = "create memory table PROJECT_MANAGER(EID varchar(256) not null, PROJECT varchar(256), primary key (EID), foreign key (EID) references MANAGER(ID))";
      st.execute(projectManagerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID), "
          + "foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);


      // Records Inserting

      String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values ("
          + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','C001'),"
          + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values ("
          + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling = "insert into PROJECT_MANAGER (EID,PROJECT) values ("
          + "('M001','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values ("
          + "('E001','John Black','R001',null),"
          + "('E002','Andrew Brown','R001','M001'),"
          + "('E003','Jack Johnson','R002',null))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values ("
          + "('E002','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling = "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values ("
          + "('E003','50.00','6'))";
      st.execute(contractEmployeeFilling);

      List<String> excludedTables = new ArrayList<String>();
      excludedTables.add("RESIDENCE");

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", OFilterTableImportingTestCase.XML_TABLE_PER_SUBCLASS2, "java", null, excludedTables, context);

      
      /*
       *  Testing context information
       */

      assertEquals(8, context.getStatistics().totalNumberOfRecords);
      assertEquals(8, context.getStatistics().analyzedRecords);
      assertEquals(5, context.getStatistics().orientAddedVertices);


      /*
       * Test OrientDB Schema
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType employeeVertexType = orientGraph.getVertexType("Employee");
      OrientVertexType regularEmployeeVertexType = orientGraph.getVertexType("Regularemployee");
      OrientVertexType contractEmployeeVertexType = orientGraph.getVertexType("Contractemployee");

      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      OrientVertexType employeeSuperclass = employeeVertexType.getSuperClass();
      OrientVertexType regularEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();
      OrientVertexType contractEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();

      assertNotNull(employeeSuperclass);
      assertEquals("V", employeeSuperclass.getName());
      assertNotNull(regularEmployeeSuperclass);
      assertEquals("Employee", regularEmployeeSuperclass.getName());
      assertNotNull(contractEmployeeSuperclass);
      assertEquals("Employee", contractEmployeeSuperclass.getName());

      OrientVertexType managerVertexType = orientGraph.getVertexType("Manager");
      OrientVertexType projectManagerVertexType = orientGraph.getVertexType("ProjectManager");

      assertNotNull(managerVertexType);
      assertNotNull(projectManagerVertexType);

      OrientVertexType managerSuperclass = managerVertexType.getSuperClass();
      OrientVertexType projectManagerSuperclass = projectManagerVertexType.getSuperClass();

      assertNotNull(managerSuperclass);
      assertEquals("V", managerSuperclass.getName());
      assertNotNull(projectManagerSuperclass);
      assertEquals("Manager", projectManagerSuperclass.getName());


      /*
       *  Testing built OrientDB
       */

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(5, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("RegularEmployee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("ContractEmployee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Manager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("ProjectManager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasManager")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);


      // vertex properties and connections check

      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"E001"};

      OrientVertex v = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(Direction.OUT, "HasRESIDENCE").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E002";
      iterator = orientGraph.getVertices("RegularEmployee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("10", v.getProperty("bonus").toString());
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasMANAGER").iterator();
        assertEquals("M001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E003";
      iterator = orientGraph.getVertices("ContractEmployee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertEquals("50.00", v.getProperty("payPerHour").toString());
        assertEquals("6", v.getProperty("contractDuration").toString());

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "M001";
      iterator = orientGraph.getVertices("Manager", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }
      
      values[0] = "C001";
      iterator = orientGraph.getVertices("Country", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("C001", v.getProperty("id"));
        assertEquals("Italy", v.getProperty("name"));
        assertEquals("Europe", v.getProperty("continent"));

        edgesIt = v.getEdges(Direction.IN).iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }



    } catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }


  @Test
  /*
   * Filtering out a table through include-tables (with Table per Concrete Type inheritance).
   */
  public void test6() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String countryTableBuilding = "create memory table COUNTRY(ID varchar(256) not null, NAME varchar(256), CONTINENT varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(countryTableBuilding);

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, NAME varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String projectManagerTableBuilding = "create memory table PROJECT_MANAGER(ID varchar(256) not null, NAME varchar(256), PROJECT varchar(256), primary key (ID))";
      st.execute(projectManagerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), primary key (ID), "
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

      String countryFilling = "insert into COUNTRY (ID,NAME,CONTINENT) values ("
          + "('C001','Italy','Europe'))";
      st.execute(countryFilling);

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','C001'),"
          + "('R002','Milan','C001'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values ("
          + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling = "insert into PROJECT_MANAGER (ID,NAME,PROJECT) values ("
          + "('M001','Bill Right','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values ("
          + "('E001','John Black','R001',null),"
          + "('E002','Andrew Brown','R001','M001'),"
          + "('E003','Jack Johnson','R002',null))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,SALARY,BONUS) values ("
          + "('E002','Andrew Brown','R001','M001','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling = "insert into CONTRACT_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,PAY_PER_HOUR,CONTRACT_DURATION) values ("
          + "('E003','Jack Johnson','R002',null,'50.00','6'))";
      st.execute(contractEmployeeFilling);

      List<String> includedTables = new ArrayList<String>();
      includedTables.add("COUNTRY");
      includedTables.add("MANAGER");
      includedTables.add("PROJECT_MANAGER");
      includedTables.add("EMPLOYEE");
      includedTables.add("REGULAR_EMPLOYEE");
      includedTables.add("CONTRACT_EMPLOYEE");

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", OFilterTableImportingTestCase.XML_TABLE_PER_CONCRETE_CLASS, "java", includedTables, null, context);

      /*
       *  Testing context information
       */

      assertEquals(8, context.getStatistics().totalNumberOfRecords);
      assertEquals(8, context.getStatistics().analyzedRecords);
      assertEquals(5, context.getStatistics().orientAddedVertices);


      /*
       * Test OrientDB Schema
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType employeeVertexType = orientGraph.getVertexType("Employee");
      OrientVertexType regularEmployeeVertexType = orientGraph.getVertexType("RegularEmployee");
      OrientVertexType contractEmployeeVertexType = orientGraph.getVertexType("ContractEmployee");

      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      OrientVertexType employeeSuperclass = employeeVertexType.getSuperClass();
      OrientVertexType regularEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();
      OrientVertexType contractEmployeeSuperclass = regularEmployeeVertexType.getSuperClass();

      assertNotNull(employeeSuperclass);
      assertEquals("V", employeeSuperclass.getName());
      assertNotNull(regularEmployeeSuperclass);
      assertEquals("Employee", regularEmployeeSuperclass.getName());
      assertNotNull(contractEmployeeSuperclass);
      assertEquals("Employee", contractEmployeeSuperclass.getName());

      OrientVertexType managerVertexType = orientGraph.getVertexType("Manager");
      OrientVertexType projectManagerVertexType = orientGraph.getVertexType("ProjectManager");

      assertNotNull(managerVertexType);
      assertNotNull(projectManagerVertexType);

      OrientVertexType managerSuperclass = managerVertexType.getSuperClass();
      OrientVertexType projectManagerSuperclass = projectManagerVertexType.getSuperClass();

      assertNotNull(managerSuperclass);
      assertEquals("V", managerSuperclass.getName());
      assertNotNull(projectManagerSuperclass);
      assertEquals("Manager", projectManagerSuperclass.getName());


      /*
       *  Testing built OrientDB
       */

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(5, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("RegularEmployee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("ContractEmployee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Manager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("ProjectManager")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(1, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasManager")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(1, count);


      // vertex properties and connections check

      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"E001"};

      OrientVertex v = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E001", v.getProperty("id"));
        assertEquals("John Black", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E002";
      iterator = orientGraph.getVertices("RegularEmployee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E002", v.getProperty("id"));
        assertEquals("Andrew Brown", v.getProperty("name"));
        assertEquals("R001", v.getProperty("residence"));
        assertEquals("1000.00", v.getProperty("salary").toString());
        assertEquals("10", v.getProperty("bonus").toString());
        assertNull(v.getProperty("payPerHour"));
        assertNull(v.getProperty("contractPeriod"));

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasManager").iterator();
        assertEquals("M001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }


      values[0] = "E003";
      iterator = orientGraph.getVertices("ContractEmployee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E003", v.getProperty("id"));
        assertEquals("Jack Johnson", v.getProperty("name"));
        assertEquals("R002", v.getProperty("residence"));
        assertNull(v.getProperty("salary"));
        assertNull(v.getProperty("bonus"));
        assertEquals("50.00", v.getProperty("payPerHour").toString());
        assertEquals("6", v.getProperty("contractDuration").toString());

        edgesIt = v.getEdges(Direction.OUT, "HasResidence").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "M001";
      iterator = orientGraph.getVertices("Manager", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("M001", v.getProperty("id"));
        assertEquals("Bill Right", v.getProperty("name"));
        assertEquals("New World", v.getProperty("project"));

        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }
      
      values[0] = "C001";
      iterator = orientGraph.getVertices("Country", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("C001", v.getProperty("id"));
        assertEquals("Italy", v.getProperty("name"));
        assertEquals("Europe", v.getProperty("continent"));

        edgesIt = v.getEdges(Direction.IN).iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }



    } catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }




}
