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

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveAggregationStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.orientechnologies.teleporter.util.OFileManager;
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
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class ImportWithInputRelationshipConfigurationTest {

  private OTeleporterContext            context;
  private ODBMSNaiveStrategy            naiveStrategy;
  private ODBMSNaiveAggregationStrategy naiveAggregationStrategy;
  private String                        dbParentDirectoryPath;
  private final String configDirectEdgesPath = "src/test/resources/configuration-mapping/relationships-mapping-direct-edges.json";
  private final String configInverseEdgesPath = "src/test/resources/configuration-mapping/relationships-mapping-inverted-edges.json";
  private final String configJoinTableDirectEdgesPath = "src/test/resources/configuration-mapping/joint-table-relationships-mapping-direct-edges.json";
  private final String configJoinTableInverseEdgesPath = "src/test/resources/configuration-mapping/joint-table-relationships-mapping-inverted-edges.json";
  private final String configJoinTableInverseEdgesPath2 = "src/test/resources/configuration-mapping/join-table-relationship-mapping-inverted-edges2.json";
  private ODBQueryEngine dbQueryEngine;
  private String driver = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private String outOrientGraphUri = "memory:testOrientDB";
  private OSourceDatabaseInfo sourceDBInfo;


  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.dbQueryEngine = new ODBQueryEngine(this.driver, this.context);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.naiveStrategy = new ODBMSNaiveStrategy();
    this.naiveAggregationStrategy = new ODBMSNaiveAggregationStrategy();
    this.outOrientGraphUri = "plocal:target/testOrientDB";
    this.dbParentDirectoryPath = this.outOrientGraphUri.replace("plocal:","");
    this.sourceDBInfo = new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
  }


  @Test

  /*
   *  Two tables: 2 relationships not declared through foreign keys.
   *  EMPLOYEE --[WorksAtProject]--> PROJECT
   *  PROJECT --[HasManager]--> EMPLOYEE
   *
   *  Properties manually configured on edges:
   *
   *  * WorksAtProject:
   *    - updatedOn (type DATE): mandatory=T, readOnly=F, notNull=F.
   *    - propWithoutTypeField (type not present in config --> property will be dropped): mandatory=T, readOnly=F, notNull=F.
   *  * HasManager:
   *    - updatedOn (type DATE): mandatory=F.
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

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

      this.naiveStrategy
              .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, this.configDirectEdgesPath, context);


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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        OFileManager.deleteFile(this.dbParentDirectoryPath);
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
   *  Two tables: 2 relationships declared through foreign keys but the first one is overridden through a migrationConfigDoc.
   *  Changes on the final edge:
   *  - name
   *  - direction inverted
   *  - property added
   *
   *  EMPLOYEE: foreign key (PROJECT) references PROJECT(ID)
   *  PROJECT: foreign key (PROJECT_MANAGER) references EMPLOYEE(EMP_ID)
   *
   *  With default mapping we would obtain:
   *
   *  EMPLOYEE --[HasProject]--> PROJECT
   *  PROJECT --[HasProjectManager]--> EMPLOYEE
   *
   *  But through migrationConfigDoc we obtain:
   *
   *  PROJECT --[HasEmployee]--> EMPLOYEE
   *  PROJECT --[HasProjectManager]--> EMPLOYEE
   *
   *  Properties manually configured on edges:
   *
   *  * HasEmployee:
   *    - updatedOn (type DATE): mandatory=T, readOnly=F, notNull=F.
   *    - propWithoutTypeField (type not present in config --> property will be dropped): mandatory=T, readOnly=F, notNull=F.
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

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

      this.naiveStrategy
              .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, this.configInverseEdgesPath, context);


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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        assertEquals(false, edgesIt.hasNext());
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
        OFileManager.deleteFile(this.dbParentDirectoryPath);
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
   *  Three tables: 1  N-N relationship, no foreign keys declared for the join table in the db.
   *  Through the migrationConfigDoc we obtain the following schema:
   *
   *  ACTOR
   *  FILM
   *  ACTOR2FILM: foreign key (ACTOR_ID) references ACTOR(ID)
   *              foreign key (FILM_ID) references FILM(ID)
   *
   *  With "direct" direction in the migrationConfigDoc we obtain:
   *
   *  ACTOR --[Performs]--> FILM
   *
   *  Properties manually configured on edges:
   *
   *  Performs:
   *    - year (type DATE): mandatory=T, readOnly=F, notNull=F.
   */

  public void test3() {

    this.context.setExecutionStrategy("naive-aggregate");
    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table ACTOR (ID varchar(256) not null,"+
              " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table FILM (ID varchar(256),"+
              " TITLE varchar(256) not null, CATEGORY varchar(256), primary key (ID))";
      st.execute(foreignTableBuilding);

      String actorFilmTableBuilding = "create memory table ACTOR_FILM (ACTOR_ID  varchar(256),"+
              " FILM_ID varchar(256) not null, PAYMENT integer, primary key (ACTOR_ID, FILM_ID))";
      st.execute(actorFilmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,CATEGORY) values ("
              + "('F001','Pulp Fiction','Action'),"
              + "('F002','Shutter Island','Thriller'),"
              + "('F003','The Departed','Action-Thriller'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,FIRST_NAME,LAST_NAME) values ("
              + "('A001','John','Travolta'),"
              + "('A002','Samuel','Lee Jackson'),"
              + "('A003','Bruce','Willis'),"
              + "('A004','Leonardo','Di Caprio'),"
              + "('A005','Ben','Kingsley'),"
              + "('A006','Mark','Ruffalo'),"
              + "('A007','Jack','Nicholson'),"
              + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into ACTOR_FILM (ACTOR_ID,FILM_ID,PAYMENT) values ("
              + "('A001','F001','12000000'),"
              + "('A002','F001','10000000'),"
              + "('A003','F001','15000000'),"
              + "('A004','F002','30000000'),"
              + "('A004','F003','40000000'),"
              + "('A005','F002','35000000'),"
              + "('A006','F002','9000000'),"
              + "('A007','F003','25000000'),"
              + "('A008','F003','15000000'))";
      st.execute(film2actorFilling);

      this.naiveAggregationStrategy
              .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, this.configJoinTableDirectEdgesPath, context);

      /*
       *  Testing context information
       */

      assertEquals(20, context.getStatistics().totalNumberOfRecords);
      assertEquals(20, context.getStatistics().analyzedRecords);
      assertEquals(11, context.getStatistics().orientAddedVertices);
      assertEquals(9, context.getStatistics().orientAddedEdges);


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
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("Performs")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);


      // vertex properties and connections check
      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"F001"};

      Vertex v = null;
      Edge currentEdge = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("Pulp Fiction", v.getProperty("title"));
        assertEquals("Action", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(12000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(10000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(30000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(35000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(9000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Action-Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(40000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(25000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A008", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("Travolta", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(12000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Samuel", v.getProperty("firstName"));
        assertEquals("Lee Jackson", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(10000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Bruce", v.getProperty("firstName"));
        assertEquals("Willis", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Leonardo", v.getProperty("firstName"));
        assertEquals("Di Caprio", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(30000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(40000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Ben", v.getProperty("firstName"));
        assertEquals("Kingsley", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(35000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Mark", v.getProperty("firstName"));
        assertEquals("Ruffalo", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(9000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Jack", v.getProperty("firstName"));
        assertEquals("Nicholson", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(25000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A008";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A008", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("firstName"));
        assertEquals("Damon", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        OFileManager.deleteFile(this.dbParentDirectoryPath);
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
   *  Three tables: 1  N-N relationship, no foreign keys declared for the join table in the db.
   *  Through the migrationConfigDoc we obtain the following schema:
   *
   *  ACTOR
   *  FILM
   *  ACTOR2FILM: foreign key (ACTOR_ID) references ACTOR(ID)
   *              foreign key (FILM_ID) references FILM(ID)
   *
   *  With "direct" direction in the migrationConfigDoc we would obtain:
   *
   *  FILM --[Performs]--> ACTOR
   *
   *  But with the "inverse" direction we obtain:
   *
   *  ACTOR --[Performs]--> FILM
   *
   *  Performs:
   *    - year (type DATE): mandatory=T, readOnly=F, notNull=F.
   */

  public void test4() {

    this.context.setExecutionStrategy("naive-aggregate");
    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table ACTOR (ID varchar(256) not null,"+
              " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table FILM (ID varchar(256),"+
              " TITLE varchar(256) not null, CATEGORY varchar(256), primary key (ID))";
      st.execute(foreignTableBuilding);

      String actorFilmTableBuilding = "create memory table FILM_ACTOR (FILM_ID  varchar(256),"+
              " ACTOR_ID varchar(256) not null, PAYMENT integer, primary key (FILM_ID, ACTOR_ID))";
      st.execute(actorFilmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,CATEGORY) values ("
              + "('F001','Pulp Fiction','Action'),"
              + "('F002','Shutter Island','Thriller'),"
              + "('F003','The Departed','Action-Thriller'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,FIRST_NAME,LAST_NAME) values ("
              + "('A001','John','Travolta'),"
              + "('A002','Samuel','Lee Jackson'),"
              + "('A003','Bruce','Willis'),"
              + "('A004','Leonardo','Di Caprio'),"
              + "('A005','Ben','Kingsley'),"
              + "('A006','Mark','Ruffalo'),"
              + "('A007','Jack','Nicholson'),"
              + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into FILM_ACTOR (FILM_ID,ACTOR_ID,PAYMENT) values ("
              + "('F001','A001','12000000'),"
              + "('F001','A002','10000000'),"
              + "('F001','A003','15000000'),"
              + "('F002','A004','30000000'),"
              + "('F002','A005','35000000'),"
              + "('F002','A006','9000000'),"
              + "('F003','A004','40000000'),"
              + "('F003','A007','25000000'),"
              + "('F003','A008','15000000'))";
      st.execute(film2actorFilling);

      this.naiveAggregationStrategy
              .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, this.configJoinTableInverseEdgesPath, context);

      /*
       *  Testing context information
       */

      assertEquals(20, context.getStatistics().totalNumberOfRecords);
      assertEquals(20, context.getStatistics().analyzedRecords);
      assertEquals(11, context.getStatistics().orientAddedVertices);
      assertEquals(9, context.getStatistics().orientAddedEdges);


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
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("Performs")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);


      // vertex properties and connections check
      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"F001"};

      Vertex v = null;
      Edge currentEdge = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("Pulp Fiction", v.getProperty("title"));
        assertEquals("Action", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(12000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(10000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(30000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(35000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(9000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Action-Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(40000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(25000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A008", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("Travolta", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(12000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Samuel", v.getProperty("firstName"));
        assertEquals("Lee Jackson", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(10000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Bruce", v.getProperty("firstName"));
        assertEquals("Willis", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Leonardo", v.getProperty("firstName"));
        assertEquals("Di Caprio", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(30000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(40000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Ben", v.getProperty("firstName"));
        assertEquals("Kingsley", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(35000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Mark", v.getProperty("firstName"));
        assertEquals("Ruffalo", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(9000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Jack", v.getProperty("firstName"));
        assertEquals("Nicholson", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(25000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A008";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A008", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("firstName"));
        assertEquals("Damon", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        OFileManager.deleteFile(this.dbParentDirectoryPath);
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
   *  Three tables: 1  N-N relationship, foreign keys declared for the join table in the db:
   *
   *  ACTOR
   *  FILM
   *  ACTOR2FILM: foreign key (ACTOR_ID) references ACTOR(ID)
   *              foreign key (FILM_ID) references FILM(ID)
   *
   *  Through the migrationConfigDoc we want name the relationship "Performs".
   *  With "direct" direction in the migrationConfigDoc we obtain:
   *
   *  ACTOR --[Performs]--> FILM
   *
   *  Properties manually configured on edges:
   *
   *  Performs:
   *    - year (type DATE): mandatory=T, readOnly=F, notNull=F.
   */

  public void test5() {

    this.context.setExecutionStrategy("naive-aggregate");
    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table ACTOR (ID varchar(256) not null,"+
              " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table FILM (ID varchar(256),"+
              " TITLE varchar(256) not null, CATEGORY varchar(256), primary key (ID))";
      st.execute(foreignTableBuilding);

      String actorFilmTableBuilding = "create memory table ACTOR_FILM (ACTOR_ID  varchar(256),"+
              " FILM_ID varchar(256) not null, PAYMENT integer, primary key (ACTOR_ID, FILM_ID)," +
              " foreign key (ACTOR_ID) references ACTOR(ID), foreign key (FILM_ID) references FILM(ID))";
      st.execute(actorFilmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,CATEGORY) values ("
              + "('F001','Pulp Fiction','Action'),"
              + "('F002','Shutter Island','Thriller'),"
              + "('F003','The Departed','Action-Thriller'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,FIRST_NAME,LAST_NAME) values ("
              + "('A001','John','Travolta'),"
              + "('A002','Samuel','Lee Jackson'),"
              + "('A003','Bruce','Willis'),"
              + "('A004','Leonardo','Di Caprio'),"
              + "('A005','Ben','Kingsley'),"
              + "('A006','Mark','Ruffalo'),"
              + "('A007','Jack','Nicholson'),"
              + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into ACTOR_FILM (ACTOR_ID,FILM_ID,PAYMENT) values ("
              + "('A001','F001','12000000'),"
              + "('A002','F001','10000000'),"
              + "('A003','F001','15000000'),"
              + "('A004','F002','30000000'),"
              + "('A004','F003','40000000'),"
              + "('A005','F002','35000000'),"
              + "('A006','F002','9000000'),"
              + "('A007','F003','25000000'),"
              + "('A008','F003','15000000'))";
      st.execute(film2actorFilling);

      this.naiveAggregationStrategy.executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, this.configJoinTableDirectEdgesPath, context);

      /*
       *  Testing context information
       */

      assertEquals(20, context.getStatistics().totalNumberOfRecords);
      assertEquals(20, context.getStatistics().analyzedRecords);
      assertEquals(11, context.getStatistics().orientAddedVertices);
      assertEquals(9, context.getStatistics().orientAddedEdges);


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
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("Performs")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);


      // vertex properties and connections check
      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"F001"};

      Vertex v = null;
      Edge currentEdge = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("Pulp Fiction", v.getProperty("title"));
        assertEquals("Action", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(12000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(10000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(30000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(35000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(9000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Action-Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(40000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(25000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A008", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("Travolta", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(12000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Samuel", v.getProperty("firstName"));
        assertEquals("Lee Jackson", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(10000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Bruce", v.getProperty("firstName"));
        assertEquals("Willis", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Leonardo", v.getProperty("firstName"));
        assertEquals("Di Caprio", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(30000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(40000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Ben", v.getProperty("firstName"));
        assertEquals("Kingsley", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(35000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Mark", v.getProperty("firstName"));
        assertEquals("Ruffalo", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(9000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Jack", v.getProperty("firstName"));
        assertEquals("Nicholson", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(25000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A008";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A008", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("firstName"));
        assertEquals("Damon", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        OFileManager.deleteFile(this.dbParentDirectoryPath);
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
   *  Three tables: 1  N-N relationship, foreign keys declared for the join table in the db:
   *
   *  ACTOR
   *  FILM
   *  ACTOR2FILM: foreign key (ACTOR_ID) references ACTOR(ID)
   *              foreign key (FILM_ID) references FILM(ID)
   *
   *  Through the migrationConfigDoc we want name the relationship "Performs".
   *  With "direct" direction in the migrationConfigDoc we would obtain:
   *
   *  ACTOR --[Features]--> FILM
   *
   *  But with the "inverse" direction we obtain:
   *
   *  FILM --[Features]--> ACTOR
   *
   *  Performs:
   *    - year (type DATE): mandatory=T, readOnly=F, notNull=F.
   */

  public void test6() {

    this.context.setExecutionStrategy("naive-aggregate");
    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table ACTOR (ID varchar(256) not null,"+
              " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table FILM (ID varchar(256),"+
              " TITLE varchar(256) not null, CATEGORY varchar(256), primary key (ID))";
      st.execute(foreignTableBuilding);

      String actorFilmTableBuilding = "create memory table FILM_ACTOR (ACTOR_ID  varchar(256),"+
              " FILM_ID varchar(256) not null, PAYMENT integer, primary key (ACTOR_ID, FILM_ID)," +
              " foreign key (ACTOR_ID) references ACTOR(ID), foreign key (FILM_ID) references FILM(ID))";
      st.execute(actorFilmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,CATEGORY) values ("
              + "('F001','Pulp Fiction','Action'),"
              + "('F002','Shutter Island','Thriller'),"
              + "('F003','The Departed','Action-Thriller'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,FIRST_NAME,LAST_NAME) values ("
              + "('A001','John','Travolta'),"
              + "('A002','Samuel','Lee Jackson'),"
              + "('A003','Bruce','Willis'),"
              + "('A004','Leonardo','Di Caprio'),"
              + "('A005','Ben','Kingsley'),"
              + "('A006','Mark','Ruffalo'),"
              + "('A007','Jack','Nicholson'),"
              + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into FILM_ACTOR (FILM_ID,ACTOR_ID,PAYMENT) values ("
              + "('F001','A001','12000000'),"
              + "('F001','A002','10000000'),"
              + "('F001','A003','15000000'),"
              + "('F002','A004','30000000'),"
              + "('F002','A005','35000000'),"
              + "('F002','A006','9000000'),"
              + "('F003','A004','40000000'),"
              + "('F003','A007','25000000'),"
              + "('F003','A008','15000000'))";
      st.execute(film2actorFilling);

      this.naiveAggregationStrategy.executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, this.configJoinTableInverseEdgesPath2, context);

      /*
       *  Testing context information
       */

      assertEquals(20, context.getStatistics().totalNumberOfRecords);
      assertEquals(20, context.getStatistics().analyzedRecords);
      assertEquals(11, context.getStatistics().orientAddedVertices);
      assertEquals(9, context.getStatistics().orientAddedEdges);


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
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("Features")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);


      // vertex properties and connections check
      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"F001"};

      Vertex v = null;
      Edge currentEdge = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("Pulp Fiction", v.getProperty("title"));
        assertEquals("Action", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.OUT, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(12000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(10000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.OUT, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(30000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(35000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(9000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Action-Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(Direction.OUT, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(40000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(25000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A008", currentEdge.getVertex(Direction.IN).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("Travolta", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(12000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Samuel", v.getProperty("firstName"));
        assertEquals("Lee Jackson", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(10000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Bruce", v.getProperty("firstName"));
        assertEquals("Willis", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Leonardo", v.getProperty("firstName"));
        assertEquals("Di Caprio", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(30000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(40000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Ben", v.getProperty("firstName"));
        assertEquals("Kingsley", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(35000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Mark", v.getProperty("firstName"));
        assertEquals("Ruffalo", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(9000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        assertEquals("Jack", v.getProperty("firstName"));
        assertEquals("Nicholson", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(25000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A008";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A008", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("firstName"));
        assertEquals("Damon", v.getProperty("lastName"));
        edgesIt = v.getEdges(Direction.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(Direction.OUT).getProperty("id"));
        assertEquals(15000000, currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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
        OFileManager.deleteFile(this.dbParentDirectoryPath);
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
