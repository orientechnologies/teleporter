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

package com.orientechnologies.orient.drakkar.test.inheritance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.OOutputStreamManager;
import com.orientechnologies.orient.drakkar.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.orient.drakkar.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.orient.drakkar.strategy.ONaiveImportStrategy;
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

public class OHibernateImportTestCase {

  private ODrakkarContext context;
  private ONaiveImportStrategy importStrategy;
  private String outOrientGraphUri;

  private final static String XML_MAPPING_FILE1 = "src/main/resources/inheritance/hibernate/test1.xml";
  private final static String XML_MAPPING_FILE2 = "src/main/resources/inheritance/hibernate/test2.xml";


  @Before
  public void init() {
    this.context = new ODrakkarContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.importStrategy = new ONaiveImportStrategy();
    this.outOrientGraphUri = "memory:testOrientDB";
  }


  @Test

  /*
   * Table per Subclass Inheritance (<joined-subclass> tag)
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-subclass )
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;


    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), primary key (id))";
      st = connection.createStatement();
      st.execute(employeeTableBuilding);


      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      
      // Records Inserting

      String employeeFilling = "INSERT INTO EMPLOYEE (ID,NAME) VALUES ("
          + "('E001','George Brown'),"
          + "('E002','John Black'))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "INSERT INTO REGULAR_EMPLOYEE (EID,SALARY,BONUS) VALUES ("
          + "('E001','40000.00','0.00'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling = "INSERT INTO CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) VALUES ("
          + "('E002','50.00','3'))";
      st.execute(contractEmployeeFilling);
      
      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", OHibernateImportTestCase.XML_MAPPING_FILE1, "java", context);

      
      /*
       *  Testing context information
       */

      assertEquals(4, context.getStatistics().totalNumberOfRecords);
      assertEquals(4, context.getStatistics().importedRecords);
      assertEquals(4, context.getStatistics().orientVertices);
      
      
      /*
       * Test OrientDB
       */
      
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      
      OrientVertexType employeeVertexType = orientGraph.getVertexType("Employee");
      OrientVertexType regularEmployeeVertexType = orientGraph.getVertexType("RegularEmployee");
      OrientVertexType contractEmployeeVertexType = orientGraph.getVertexType("ContractEmployee");
      
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);
      
      Set<OClass> employeeSuperclasses = (Set<OClass>) employeeVertexType.getAllSuperClasses();
      Set<OClass> employeeSubclasses = (Set<OClass>) employeeVertexType.getAllSubclasses();
      Set<OClass> regularEmployeeSuperclasses = (Set<OClass>) regularEmployeeVertexType.getAllSuperClasses();
      Set<OClass> contractEmployeeSuperclasses = (Set<OClass>) regularEmployeeVertexType.getAllSuperClasses();
      
      assertEquals(1, employeeSuperclasses.size());
      assertEquals(2, employeeSubclasses.size());
      assertEquals(2, regularEmployeeSuperclasses.size());
      assertEquals(2, contractEmployeeSuperclasses.size());
      Iterator<OClass> it = employeeSuperclasses.iterator();
      assertEquals("V", it.next().getName());
      it = employeeSubclasses.iterator();
      assertEquals("ContractEmployee", it.next().getName());
      assertEquals("RegularEmployee", it.next().getName());
      it = regularEmployeeSuperclasses.iterator();
      assertEquals("V", it.next().getName());
      assertEquals("Employee", it.next().getName());
      it = contractEmployeeSuperclasses.iterator();
      assertEquals("V", it.next().getName());
      assertEquals("Employee", it.next().getName());


      /*
       *  Testing built OrientDB
       */
      
      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

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


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("IsAs")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);


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
        assertEquals("George Brown", v.getProperty("name"));
        edgesIt = v.getEdges(Direction.IN, "IsAs").iterator();
        assertEquals("E001", edgesIt.next().getVertex(Direction.OUT).getProperty("eid"));
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
        assertEquals("John Black", v.getProperty("name"));
        edgesIt = v.getEdges(Direction.IN, "IsAs").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("eid"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E001";
      iterator = orientGraph.getVertices("RegularEmployee", keys, values).iterator();
      iterator = orientGraph.getVerticesOfClass("RegularEmployee").iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E001", v.getProperty("eid"));
        assertEquals("40000.00", v.getProperty("salary"));
        assertEquals("0.00", v.getProperty("bonus"));
        edgesIt = v.getEdges(Direction.OUT, "IsAs").iterator();
        assertEquals("E001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }
      
      values[0] = "E002";
      iterator = orientGraph.getVertices("ContractEmployee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = (OrientVertex) iterator.next();
        assertEquals("E002", v.getProperty("eid"));
        assertEquals("50.00", v.getProperty("payPerHour"));
        assertEquals("3", v.getProperty("contractDuration"));
        edgesIt = v.getEdges(Direction.OUT, "IsAs").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
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
        String dbDropping = "DROP SCHEMA PUBLIC CASCADE";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
  }


  @Test

  /*
   * Table per Subclass Inheritance (<subclass> <join/> </subclass> tags)
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-subclass )
   *  - REGULAR_CONTRACT has a foreign key on EID referencing EMPLOYEE (ID)
   *  - CONTRACT_CONTRACT has NOT a foreign key on EID referencing EMPLOYEE (ID)
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), EMPLOYEE_TYPE varchar(256), primary key (id))";
      st = connection.createStatement();
      st.execute(employeeTableBuilding);


      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID))";
      st.execute(contractEmployeeTableBuilding);


      // Records Inserting

      String employeeFilling = "INSERT INTO EMPLOYEE (ID,NAME,EMPLOYEE_TYPE) VALUES ("
          + "('E001','George Brown','R'),"
          + "('E002','John Black','C'))";
      st.execute(employeeFilling);

      String regularEmployeeFilling = "INSERT INTO REGULAR_EMPLOYEE (EID,SALARY,BONUS) VALUES ("
          + "('E001','40000.00','0.00'))";
      st.execute(regularEmployeeFilling);

      String contractEmployeeFilling = "INSERT INTO CONTRACT_EMPLOYEE (EID,PAY_PER_HOUR,CONTRACT_DURATION) VALUES ("
          + "('E002','50.00','3'))";
      st.execute(contractEmployeeFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "hibernate", OHibernateImportTestCase.XML_MAPPING_FILE1, "java", context);


      /*
       *  Testing context information
       */




      /*
       *  Testing built graph model
       */





    }catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "DROP SCHEMA PUBLIC CASCADE";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
  }



}
