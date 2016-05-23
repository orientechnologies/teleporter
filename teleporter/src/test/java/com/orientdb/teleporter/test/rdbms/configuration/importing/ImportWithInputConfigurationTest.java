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

package com.orientdb.teleporter.test.rdbms.configuration.importing;

import com.orientdb.teleporter.context.OOutputStreamManager;
import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientdb.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientdb.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class ImportWithInputConfigurationTest {

  private OTeleporterContext context;
  private ODBMSNaiveStrategy importStrategy;
  private String             outOrientGraphUri;
  private String             dbParentDirectoryPath;
  private final String configDirectEdgesPath = "src/test/resources/configuration-mapping/relationships-mapping-direct-edges.json";
  private final String configInverseEdgesPath = "src/test/resources/configuration-mapping/relationships-mapping-inverted-edges.json";

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.importStrategy = new ODBMSNaiveStrategy();
    this.outOrientGraphUri = "plocal:target/testOrientDB";
    this.dbParentDirectoryPath = this.outOrientGraphUri.replace("plocal:","");
  }


  @Test

  /*
   *  Two tables: 2 relationships not declared through foreign keys.
   *  EMPLOYEE --[WorksAtProject]--> PROJECT
   *  PROJECT --[HasManager]--> EMPLOYEE
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String parentTableBuilding = "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"+
          " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, PROJECT varchar(256) not null, primary key (EMP_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table PROJECT (ID  varchar(256),"+
          " TITLE varchar(256) not null, PROJECT_MANAGER varchar(256) not null, primary key (ID))";
      st.execute(foreignTableBuilding);


      // Records Inserting

      String employeeFilling = "insert into EMPLOYEE (EMP_ID,FIRST_NAME,LAST_NAME,PROJECT) values ("
          + "('E001','Joe','Black','P001'),"
          + "('E002','Thomas','Anderson','P002'),"
          + "('E003','Tyler','Durden','P001'),"
          + "('E004','John','McClanenei','P001'),"
          + "('E005','Ellen','Ripley','P002'),"
          + "('E006','Marty','McFly','P002'))";
      st.execute(employeeFilling);

      String projectFilling = "insert into PROJECT (ID,TITLE,PROJECT_MANAGER) values ("
          + "('P001','Data Migration','E001'),"
          + "('P002','Contracts Update','E005'))";
      st.execute(projectFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, this.configDirectEdgesPath, context);


      /*
       *  Testing context information
       */

      assertEquals(8, context.getStatistics().totalNumberOfRecords);
      assertEquals(8, context.getStatistics().analyzedRecords);
      assertEquals(8, context.getStatistics().orientAddedVertices);
      assertEquals(8, context.getStatistics().orientAddedEdges);


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
      assertEquals(8, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(6, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Project")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(8, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("WorksAtProject")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(6, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasManager")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);


      // vertex properties and connections check
      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"P001"};

      Vertex v = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Project", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("P001", v.getProperty("id"));
        assertEquals("Data Migration", v.getProperty("title"));
        assertEquals("E001", v.getProperty("projectManager"));
        edgesIt = v.getEdges(Direction.IN, "WorksAtProject").iterator();
        assertEquals("E001", edgesIt.next().getVertex(Direction.OUT).getProperty("empId"));
        assertEquals("E003", edgesIt.next().getVertex(Direction.OUT).getProperty("empId"));
        assertEquals("E004", edgesIt.next().getVertex(Direction.OUT).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasManager").iterator();
        assertEquals("E001", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());

      }
      else {
        fail("Query fail!");
      }

      values[0] = "P002";
      iterator = orientGraph.getVertices("Project", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("P002", v.getProperty("id"));
        assertEquals("Contracts Update", v.getProperty("title"));
        assertEquals("E005", v.getProperty("projectManager"));
        edgesIt = v.getEdges(Direction.IN, "WorksAtProject").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.OUT).getProperty("empId"));
        assertEquals("E005", edgesIt.next().getVertex(Direction.OUT).getProperty("empId"));
        assertEquals("E006", edgesIt.next().getVertex(Direction.OUT).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasManager").iterator();
        assertEquals("E005", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      keys[0] = "empId";
      values[0] = "E001";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E001", v.getProperty("empId"));
        assertEquals("Joe", v.getProperty("firstName"));
        assertEquals("Black", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.OUT, "WorksAtProject").iterator();
        assertEquals("P001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals("P001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E002";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E002", v.getProperty("empId"));
        assertEquals("Thomas", v.getProperty("firstName"));
        assertEquals("Anderson", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.OUT, "WorksAtProject").iterator();
        assertEquals("P002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E003";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E003", v.getProperty("empId"));
        assertEquals("Tyler", v.getProperty("firstName"));
        assertEquals("Durden", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.OUT, "WorksAtProject").iterator();
        assertEquals("P001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E004";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E004", v.getProperty("empId"));
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("McClanenei", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.OUT, "WorksAtProject").iterator();
        assertEquals("P001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E005";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E005", v.getProperty("empId"));
        assertEquals("Ellen", v.getProperty("firstName"));
        assertEquals("Ripley", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.OUT, "WorksAtProject").iterator();
        assertEquals("P002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
        assertEquals("P002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E006";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E006", v.getProperty("empId"));
        assertEquals("Marty", v.getProperty("firstName"));
        assertEquals("McFly", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.OUT, "WorksAtProject").iterator();
        assertEquals("P002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasManager").iterator();
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
        this.deleteFile(this.dbParentDirectoryPath);
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
   *  Two tables: 2 relationships declared through foreign keys but the first one is overridden through a configuration.
   *  Changes on the final edge:
   *  - name
   *  - direction inverted
   *  - property added
   *
   *  EMPLOYEE: foreign key (PROJECT) references PROJECT(ID)
   *  PROJECT: foreign key (PROJECT_MANAGER) references EMPLOYEE(EMP_ID)
   *
   *  With default mapping we would have:
   *
   *  EMPLOYEE --[HasProject]--> PROJECT
   *  PROJECT --[HasProjectManager]--> EMPLOYEE
   *
   *  But through configuration we obtain:
   *
   *  PROJECT --[HasEmployee]--> EMPLOYEE
   *  PROJECT --[HasProjectManager]--> EMPLOYEE
   *
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String parentTableBuilding = "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"+
          " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, PROJECT varchar(256) not null, primary key (EMP_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table PROJECT (ID  varchar(256),"+
          " TITLE varchar(256) not null, PROJECT_MANAGER varchar(256) not null, primary key (ID), "
          + "foreign key (PROJECT_MANAGER) references EMPLOYEE(EMP_ID))";
      st.execute(foreignTableBuilding);

      // Records Inserting

      String employeeFilling = "insert into EMPLOYEE (EMP_ID,FIRST_NAME,LAST_NAME,PROJECT) values ("
          + "('E001','Joe','Black','P001'),"
          + "('E002','Thomas','Anderson','P002'),"
          + "('E003','Tyler','Durden','P001'),"
          + "('E004','John','McClanenei','P001'),"
          + "('E005','Ellen','Ripley','P002'),"
          + "('E006','Marty','McFly','P002'))";
      st.execute(employeeFilling);

      String projectFilling = "insert into PROJECT (ID,TITLE,PROJECT_MANAGER) values ("
          + "('P001','Data Migration','E001'),"
          + "('P002','Contracts Update','E005'))";
      st.execute(projectFilling);

      parentTableBuilding = "alter table EMPLOYEE add foreign key (PROJECT) references PROJECT(ID)";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, this.configInverseEdgesPath, context);


      /*
       *  Testing context information
       */

      assertEquals(8, context.getStatistics().totalNumberOfRecords);
      assertEquals(8, context.getStatistics().analyzedRecords);
      assertEquals(8, context.getStatistics().orientAddedVertices);
      assertEquals(8, context.getStatistics().orientAddedEdges);


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
      assertEquals(8, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(6, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Project")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(8, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasEmployee")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(6, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasProjectManager")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);


      // vertex properties and connections check
      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"P001"};

      Vertex v = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Project", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("P001", v.getProperty("id"));
        assertEquals("Data Migration", v.getProperty("title"));
        assertEquals("E001", v.getProperty("projectManager"));
        edgesIt = v.getEdges(Direction.OUT, "HasEmployee").iterator();
        assertEquals("E001", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals("E003", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals("E004", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasProjectManager").iterator();
        assertEquals("E001", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());

      }
      else {
        fail("Query fail!");
      }

      values[0] = "P002";
      iterator = orientGraph.getVertices("Project", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("P002", v.getProperty("id"));
        assertEquals("Contracts Update", v.getProperty("title"));
        assertEquals("E005", v.getProperty("projectManager"));
        edgesIt = v.getEdges(Direction.OUT, "HasEmployee").iterator();
        assertEquals("E002", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals("E005", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals("E006", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(Direction.OUT, "HasProjectManager").iterator();
        assertEquals("E005", edgesIt.next().getVertex(Direction.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      keys[0] = "empId";
      values[0] = "E001";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E001", v.getProperty("empId"));
        assertEquals("Joe", v.getProperty("firstName"));
        assertEquals("Black", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.IN, "HasEmployee").iterator();
        assertEquals("P001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasProjectManager").iterator();
        assertEquals("P001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E002";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E002", v.getProperty("empId"));
        assertEquals("Thomas", v.getProperty("firstName"));
        assertEquals("Anderson", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.IN, "HasEmployee").iterator();
        assertEquals("P002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasProjectManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E003";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E003", v.getProperty("empId"));
        assertEquals("Tyler", v.getProperty("firstName"));
        assertEquals("Durden", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.IN, "HasEmployee").iterator();
        assertEquals("P001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasProjectManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E004";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E004", v.getProperty("empId"));
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("McClanenei", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.IN, "HasEmployee").iterator();
        assertEquals("P001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasProjectManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E005";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E005", v.getProperty("empId"));
        assertEquals("Ellen", v.getProperty("firstName"));
        assertEquals("Ripley", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.IN, "HasEmployee").iterator();
        assertEquals("P002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasProjectManager").iterator();
        assertEquals("P002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "E006";
      iterator = orientGraph.getVertices("Employee", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("E006", v.getProperty("empId"));
        assertEquals("Marty", v.getProperty("firstName"));
        assertEquals("McFly", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(Direction.IN, "HasEmployee").iterator();
        assertEquals("P002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        edgesIt = v.getEdges(Direction.IN, "HasProjectManager").iterator();
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
        this.deleteFile(this.dbParentDirectoryPath);
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

  private void deleteFile(String resourcePath) throws IOException {

    File currentFile = new File(resourcePath);
    if(currentFile.isDirectory()) {
      File[] innerFiles = currentFile.listFiles();
      for(File file: innerFiles) {
        deleteFile(file.getCanonicalPath());
      }
    }
    if(!currentFile.delete())
      throw new IOException();
  }

}
