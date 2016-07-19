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

package com.orientechnologies.teleporter.test.rdbms.aggregation;

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveAggregationStrategy;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
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

public class AggregationStrategyTest {

  private OTeleporterContext context;
  private ODBMSNaiveAggregationStrategy importStrategy;
  private String outOrientGraphUri;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.importStrategy = new ODBMSNaiveAggregationStrategy();
    this.outOrientGraphUri = "memory:testOrientDB";
  }


  @Test
  /*
   * Aggregation Strategy Test: executing import
   */
  public void test1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String filmTableBuilding = "create memory table film (id varchar(256) not null,"+
          " title varchar(256) not null, primary key (id))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      String actorTableBuilding = "create memory table actor (id varchar(256) not null, name  varchar(256),"+
          " surname varchar(256) not null, primary key (id))";
      st.execute(actorTableBuilding);

      String film2actorTableBuilding = "create memory table film_actor (film_id varchar(256) not null, actor_id  varchar(256), PAYMENT integer, "+
          " primary key (film_id,actor_id), foreign key (film_id) references film(id), foreign key (actor_id) references actor(id))";
      st.execute(film2actorTableBuilding);


      // Records Inserting

      String filmFilling = "insert into film(id,title) values ("
          + "('F001','The Wolf Of Wall Street'),"
          + "('F002','Shutter Island'),"
          + "('F003','The Departed'),"
          + "('F004','Inception'))";
      st.execute(filmFilling);

      String actorFilling = "insert into actor (id,name,surname) values ("
          + "('A001','Leonardo','Di Caprio'),"
          + "('A002','Matthew', 'McConaughey'),"
          + "('A003','Ben','Kingsley'),"
          + "('A004','Mark','Ruffalo'),"
          + "('A005','Jack','Nicholson'),"
          + "('A006','Matt','Damon'),"
          + "('A007','Michael','Caine'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into film_actor (film_id,actor_id,payment) values ("
          + "('F001','A001','32000000'),"
          + "('F001','A002','20000000'),"
          + "('F002','A001','28000000'),"
          + "('F002','A003','18000000'),"
          + "('F002','A004','6000000'),"
          + "('F003','A001','25000000'),"
          + "('F003','A005','27000000'),"
          + "('F003','A006','14000000'),"
          + "('F004','A001','30000000'),"
          + "('F004','A007','12000000'))";
      st.execute(film2actorFilling);


      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null, context);


      /*
       *  Testing context information
       */

      assertEquals(21, context.getStatistics().totalNumberOfRecords);
      assertEquals(21, context.getStatistics().analyzedRecords);
      assertEquals(11, context.getStatistics().orientAddedVertices);
      assertEquals(10, context.getStatistics().orientAddedEdges);

      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(11, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(7, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(10, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("FilmActor")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(10, count);


      // vertex properties and connections check
      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"F001"};

      Vertex v = null;
      Edge currentEdge;
      Iterator<Vertex> iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("The Wolf Of Wall Street", v.getProperty("title"));
        edgesIt = v.getEdges(Direction.IN, "FilmActor").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(32000000, currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(20000000, currentEdge.getProperty("payment"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F002";
      iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F002", v.getProperty("id"));
        assertEquals("Shutter Island", v.getProperty("title"));
        edgesIt = v.getEdges(Direction.IN, "FilmActor").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(28000000, currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(18000000, currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(6000000, currentEdge.getProperty("payment"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F003";
      iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F003", v.getProperty("id"));
        assertEquals("The Departed", v.getProperty("title"));
        edgesIt = v.getEdges(Direction.IN, "FilmActor").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(25000000, currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(27000000, currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(14000000, currentEdge.getProperty("payment"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F004";
      iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F004", v.getProperty("id"));
        assertEquals("Inception", v.getProperty("title"));
        edgesIt = v.getEdges(Direction.IN, "FilmActor").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(30000000, currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(12000000, currentEdge.getProperty("payment"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A001";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A001", v.getProperty("id"));
        assertEquals("Leonardo", v.getProperty("name"));
        assertEquals("Di Caprio", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "FilmActor").iterator();
        assertEquals("F001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals("F002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals("F003", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals("F004", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A002";
      iterator =  orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A002", v.getProperty("id"));
        assertEquals("Matthew", v.getProperty("name"));
        assertEquals("McConaughey", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "FilmActor").iterator();
        assertEquals("F001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A003";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A003", v.getProperty("id"));
        assertEquals("Ben", v.getProperty("name"));
        assertEquals("Kingsley", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "FilmActor").iterator();
        assertEquals("F002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A004";
      iterator =  orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A004", v.getProperty("id"));
        assertEquals("Mark", v.getProperty("name"));
        assertEquals("Ruffalo", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "FilmActor").iterator();
        assertEquals("F002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A005";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A005", v.getProperty("id"));
        assertEquals("Jack", v.getProperty("name"));
        assertEquals("Nicholson", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "FilmActor").iterator();
        assertEquals("F003", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A006";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A006", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("name"));
        assertEquals("Damon", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "FilmActor").iterator();
        assertEquals("F003", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A007";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A007", v.getProperty("id"));
        assertEquals("Michael", v.getProperty("name"));
        assertEquals("Caine", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "FilmActor").iterator();
        assertEquals("F004", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

    } catch(Exception e) {
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
   * Aggregation Strategy Test: executing mapping
   */
  public void test2() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(employeeTableBuilding);

      String departmentTableBuilding = "create memory table DEPARTMENT (ID varchar(256) not null, NAME  varchar(256),"+
          " primary key (ID))";
      st.execute(departmentTableBuilding);

      String dept2empTableBuilding = "create memory table DEPT_EMP (DEPT_ID varchar(256) not null, EMP_ID  varchar(256), HIRING_YEAR varchar(256),"+
          " primary key (DEPT_ID,EMP_ID), foreign key (EMP_ID) references EMPLOYEE(ID), foreign key (DEPT_ID) references DEPARTMENT(ID))";
      st.execute(dept2empTableBuilding);

      String dept2managerTableBuilding = "create memory table DEPT_MANAGER (DEPT_ID varchar(256) not null, EMP_ID  varchar(256),"+
          " primary key (DEPT_ID,EMP_ID), foreign key (EMP_ID) references EMPLOYEE(ID), foreign key (DEPT_ID) references DEPARTMENT(ID))";
      st.execute(dept2managerTableBuilding);

      String branchTableBuilding = "create memory table BRANCH(BRANCH_ID varchar(256) not null, LOCATION  varchar(256),"+
          "DEPT varchar(256) not null, primary key (BRANCH_ID), foreign key (DEPT) references DEPARTMENT(ID))";
      st.execute(branchTableBuilding);


      OER2GraphMapper mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null, null);
      context.setQueryQuoteType("\"");
      mapper.buildSourceDatabaseSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(5, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(5, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType departmentVertexType = mapper.getGraphModel().getVertexTypeByName("Department");
      OVertexType deptEmpVertexType = mapper.getGraphModel().getVertexTypeByName("DeptEmp");
      OVertexType deptManagerVertexType = mapper.getGraphModel().getVertexTypeByName("DeptManager");
      OVertexType branchVertexType = mapper.getGraphModel().getVertexTypeByName("Branch");
      OEdgeType deptEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasDept");
      OEdgeType empEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasEmp");

      // vertices check
      assertEquals(5, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(departmentVertexType);
      assertNotNull(deptEmpVertexType);
      assertNotNull(deptManagerVertexType);
      assertNotNull(branchVertexType);

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(deptEdgeType);
      assertNotNull(empEdgeType);
      assertEquals(3, deptEdgeType.getNumberRelationshipsRepresented());
      assertEquals(2, empEdgeType.getNumberRelationshipsRepresented());
      
      
      /*
       * Aggregation of join tables
       */
      mapper.performMany2ManyAggregation(context);
      
      
      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(3, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(3, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      departmentVertexType = mapper.getGraphModel().getVertexTypeByName("Department");
      deptEmpVertexType = mapper.getGraphModel().getVertexTypeByName("DeptEmp");
      deptManagerVertexType = mapper.getGraphModel().getVertexTypeByName("DeptManager");
      branchVertexType = mapper.getGraphModel().getVertexTypeByName("Branch");
      deptEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasDept");
      empEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasEmp");
      OEdgeType deptEmpEdgeType = mapper.getGraphModel().getEdgeTypeByName("DeptEmp");
      OEdgeType deptManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("DeptManager");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(departmentVertexType);
      assertNull(deptEmpVertexType);
      assertNull(deptManagerVertexType);
      assertNotNull(branchVertexType);

      // edges check
      assertEquals(3, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(deptEdgeType);
      assertNotNull(deptEmpEdgeType);
      assertNotNull(deptManagerEdgeType);
      assertNull(empEdgeType);
      assertEquals(1, deptEdgeType.getNumberRelationshipsRepresented());
      assertEquals(1, deptEmpEdgeType.getNumberRelationshipsRepresented());
      assertEquals(1, deptManagerEdgeType.getNumberRelationshipsRepresented());

      assertNotNull(deptEmpEdgeType.getPropertyByName("hiringYear"));
      assertTrue(deptEmpEdgeType.getPropertyByName("hiringYear").getPropertyType().equals("VARCHAR"));

    } catch(Exception e) {
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
    }
  }

}
