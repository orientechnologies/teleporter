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

package com.orientechnologies.teleporter.test.rdbms.inheritance.hibernate;

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class HibernateImportTest {

  private OTeleporterContext context;
  private ODBMSNaiveStrategy importStrategy;
  private String outOrientGraphUri;

  private final static String XML_TABLE_PER_CLASS = "src/test/resources/inheritance/hibernate/tablePerClassHierarchyImportTest.xml";
  private final static String XML_TABLE_PER_SUBCLASS1 = "src/test/resources/inheritance/hibernate/tablePerSubclassImportTest1.xml";
  private final static String XML_TABLE_PER_SUBCLASS2 = "src/test/resources/inheritance/hibernate/tablePerSubclassImportTest2.xml";
  private final static String XML_TABLE_PER_CONCRETE_CLASS = "src/test/resources/inheritance/hibernate/tablePerConcreteClassImportTest.xml";


  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.context.setQueryQuoteType("\"");
    this.importStrategy = new ODBMSNaiveStrategy();
    this.outOrientGraphUri = "memory:testOrientDB";
  }

  @Test
  /*
   * Import from tables with "table-per-hierarchy" inheritance strategy.
   * Relationships both to simple table and table in a hierarchical bag ("table-per-hierarchy" bag).
   */
  public void test1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;


    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String residenceTableBuilding = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residenceTableBuilding);

      String managerTableBuilding = "create memory table MANAGER(ID varchar(256) not null, TYPE varchar(256), NAME varchar(256), PROJECT varchar(256), primary key (ID))";
      st.execute(managerTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " TYPE varchar(256), NAME varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), RESIDENCE varchar(256), MANAGER varchar(256), "
          + "primary key (id), foreign key (RESIDENCE) references RESIDENCE(ID), foreign key (MANAGER) references MANAGER(ID))";
      st.execute(employeeTableBuilding);


      // Records Inserting

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','Italy'),"
          + "('R002','Milan','Italy'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into manager (ID,TYPE,NAME,PROJECT) values ("
          + "('M001','prj_mgr','Bill Right','New World'))";
      st.execute(managerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,TYPE,NAME,SALARY,BONUS,PAY_PER_HOUR,CONTRACT_DURATION,RESIDENCE,MANAGER) values ("
          + "('E001','emp','John Black',NULL,NULL,NULL,NULL,'R001',NULL),"
          + "('E002','reg_emp','Andrew Brown','1000.00','10',NULL,NULL,'R001','M001'),"
          + "('E003','cont_emp','Jack Johnson',NULL,NULL,'50.00','6','R002',NULL))";
      st.execute(employeeFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", HibernateImportTest.XML_TABLE_PER_CLASS, "java", null, null, null, context);

      /*
       *  Testing context information
       */

      assertEquals(6, context.getStatistics().totalNumberOfRecords);
      assertEquals(6, context.getStatistics().analyzedRecords);
      assertEquals(6, context.getStatistics().orientAddedVertices);


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
      assertEquals(6, count);

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
      for(Vertex v: orientGraph.getVerticesOfClass("Residence")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

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
      assertEquals(4, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasResidence")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(3, count);

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
        assertEquals("R001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
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
        assertEquals("R001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
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
        assertEquals("R002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "R001";
      iterator = orientGraph.getVertices("Residence", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("R001", v.getProperty("id"));
        assertEquals("Rome", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(Direction.IN, "HasResidence").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("E001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "R002";
      iterator = orientGraph.getVertices("Residence", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("R002", v.getProperty("id"));
        assertEquals("Milan", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(Direction.IN, "HasResidence").iterator();
        assertEquals("E003", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
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



    }catch(Exception e) {
      e.printStackTrace();
      fail();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
        fail();
      }
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }


  @Test
  /*
   * Import from tables with "table-per-type" inheritance strategy.
   * Relationships both to simple table and table in a hierarchical bag ("table-per-type" bag).
   */
  public void test2() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;


    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");



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

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','Italy'),"
          + "('R002','Milan','Italy'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values ("
          + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling = "insert into PROJECT_MANAGER (EID,PROJECT) values ("
          + "('M001','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values ("
          + "('E001','John Black','R001',NULL),"
          + "('E002','Andrew Brown','R001','M001'),"
          + "('E003','Jack Johnson','R002',NULL))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values ("
          + "('E002','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling = "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values ("
          + "('E003','50.00','6'))";
      st.execute(contractEmployeeFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", HibernateImportTest.XML_TABLE_PER_SUBCLASS1, "java", null, null, null, context);

      /*
       *  Testing context information
       */

      assertEquals(9, context.getStatistics().totalNumberOfRecords);
      assertEquals(9, context.getStatistics().analyzedRecords);
      assertEquals(6, context.getStatistics().orientAddedVertices);


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
      assertEquals(6, count);

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
      for(Vertex v: orientGraph.getVerticesOfClass("Residence")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

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
      assertEquals(4, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasResidence")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(3, count);

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
        assertEquals("R001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
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
        assertEquals("R001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
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
        assertEquals("R002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "R001";
      iterator = orientGraph.getVertices("Residence", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("R001", v.getProperty("id"));
        assertEquals("Rome", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(Direction.IN, "HasResidence").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("E001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "R002";
      iterator = orientGraph.getVertices("Residence", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("R002", v.getProperty("id"));
        assertEquals("Milan", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(Direction.IN, "HasResidence").iterator();
        assertEquals("E003", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
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



    }catch(Exception e) {
      e.printStackTrace();
      fail();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
        fail();
      }
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }

  @Test
  /*
   * Import from tables with "table-per-type" inheritance strategy.
   * Relationships both to simple table and table in a hierarchical bag ("table-per-type" bag).
   */
  public void test3() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;


    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");



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

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','Italy'),"
          + "('R002','Milan','Italy'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values ("
          + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling = "insert into PROJECT_MANAGER (EID,PROJECT) values ("
          + "('M001','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values ("
          + "('E001','John Black','R001',NULL),"
          + "('E002','Andrew Brown','R001','M001'),"
          + "('E003','Jack Johnson','R002',NULL))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (EID,SALARY,BONUS) values ("
          + "('E002','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling = "insert into CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) values ("
          + "('E003','50.00','6'))";
      st.execute(contractEmployeeFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", HibernateImportTest.XML_TABLE_PER_SUBCLASS2, "java", null, null, null, context);

      /*
       *  Testing context information
       */

      assertEquals(9, context.getStatistics().totalNumberOfRecords);
      assertEquals(9, context.getStatistics().analyzedRecords);
      assertEquals(6, context.getStatistics().orientAddedVertices);


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
      assertEquals(6, count);

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
      for(Vertex v: orientGraph.getVerticesOfClass("Residence")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

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
      assertEquals(4, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasResidence")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(3, count);

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
        assertEquals("R001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
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
        assertEquals("R001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
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
        assertEquals("R002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "R001";
      iterator = orientGraph.getVertices("Residence", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("R001", v.getProperty("id"));
        assertEquals("Rome", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(Direction.IN, "HasResidence").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("E001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "R002";
      iterator = orientGraph.getVertices("Residence", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("R002", v.getProperty("id"));
        assertEquals("Milan", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(Direction.IN, "HasResidence").iterator();
        assertEquals("E003", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
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



    }catch(Exception e) {
      e.printStackTrace();
      fail();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
        fail();
      }
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }

  @Test
  /*
   * Import from tables with "table-per-concrete-type" inheritance strategy.
   * Relationships both to simple table and table in a hierarchical bag ("table-per-concrete-type" bag).
   */
  public void test4() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;


    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

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

      String residenceFilling = "insert into RESIDENCE (ID,CITY,COUNTRY) values ("
          + "('R001','Rome','Italy'),"
          + "('R002','Milan','Italy'))";
      st.execute(residenceFilling);

      String managerFilling = "insert into MANAGER (ID,NAME) values ("
          + "('M001','Bill Right'))";
      st.execute(managerFilling);

      String projectManagerFilling = "insert into PROJECT_MANAGER (ID,NAME,PROJECT) values ("
          + "('M001','Bill Right','New World'))";
      st.execute(projectManagerFilling);

      String employeeFilling = "insert into EMPLOYEE (ID,NAME,RESIDENCE,MANAGER) values ("
          + "('E001','John Black','R001',NULL),"
          + "('E002','Andrew Brown','R001','M001'),"
          + "('E003','Jack Johnson','R002',NULL))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "insert into REGULAR_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,SALARY,BONUS) values ("
          + "('E002','Andrew Brown','R001','M001','1000.00','10'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling = "insert into CONTRACT_EMPLOYEE (ID,NAME,RESIDENCE,MANAGER,PAY_PER_HOUR,CONTRACT_DURATION) values ("
          + "('E003','Jack Johnson','R002',NULL,'50.00','6'))";
      st.execute(contractEmployeeFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", HibernateImportTest.XML_TABLE_PER_CONCRETE_CLASS, "java", null, null, null, context);

      /*
       *  Testing context information
       */

      assertEquals(9, context.getStatistics().totalNumberOfRecords);
      assertEquals(9, context.getStatistics().analyzedRecords);
      assertEquals(6, context.getStatistics().orientAddedVertices);


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
      assertEquals(6, count);

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
      for(Vertex v: orientGraph.getVerticesOfClass("Residence")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

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
      assertEquals(4, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasResidence")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(3, count);

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
        assertEquals("R001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
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
        assertEquals("R001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
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
        assertEquals("R002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "R001";
      iterator = orientGraph.getVertices("Residence", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("R001", v.getProperty("id"));
        assertEquals("Rome", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(Direction.IN, "HasResidence").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("E001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "R002";
      iterator = orientGraph.getVertices("Residence", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("R002", v.getProperty("id"));
        assertEquals("Milan", v.getProperty("city"));
        assertEquals("Italy", v.getProperty("country"));

        edgesIt = v.getEdges(Direction.IN, "HasResidence").iterator();
        assertEquals("E003", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
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



    }catch(Exception e) {
      e.printStackTrace();
      fail();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
        fail();
      }
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }

}
