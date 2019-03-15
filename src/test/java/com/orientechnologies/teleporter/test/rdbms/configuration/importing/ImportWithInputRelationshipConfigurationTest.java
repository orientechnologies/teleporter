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

package com.orientechnologies.teleporter.test.rdbms.configuration.importing;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveAggregationStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.orientechnologies.teleporter.util.OFileManager;
import com.orientechnologies.teleporter.util.OGraphCommands;
import com.orientechnologies.teleporter.util.OMigrationConfigManager;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */

public class ImportWithInputRelationshipConfigurationTest {

  private OTeleporterContext            context;
  private ODBMSNaiveStrategy            naiveStrategy;
  private ODBMSNaiveAggregationStrategy naiveAggregationStrategy;
  private final String configDirectEdgesPath            = "src/test/resources/configuration-mapping/relationships-mapping-direct-edges.json";
  private final String configInverseEdgesPath           = "src/test/resources/configuration-mapping/relationships-mapping-inverted-edges.json";
  private final String configJoinTableDirectEdgesPath   = "src/test/resources/configuration-mapping/joint-table-relationships-mapping-direct-edges.json";
  private final String configJoinTableInverseEdgesPath  = "src/test/resources/configuration-mapping/joint-table-relationships-mapping-inverted-edges.json";
  private final String configJoinTableInverseEdgesPath2 = "src/test/resources/configuration-mapping/join-table-relationship-mapping-inverted-edges2.json";
  private ODBQueryEngine dbQueryEngine;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private String dbName = "testOrientDB";
  private String protocol = "embedded:";
  private String outParentDirectory = "target/";
  private String outOrientGraphUri = this.protocol + this.outParentDirectory + this.dbName;
  private OSourceDatabaseInfo sourceDBInfo;

  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance(this.protocol + this.outParentDirectory);
    this.context.initOrientDBInstance(this.protocol + this.outParentDirectory);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setMessageHandler(new OTeleporterMessageHandler(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.naiveStrategy = new ODBMSNaiveStrategy("embedded", this.outParentDirectory, this.dbName);
    this.naiveAggregationStrategy = new ODBMSNaiveAggregationStrategy("embedded", this.outParentDirectory, this.dbName);
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
   *  Two tables: 2 relationships not declared through foreign keys.
   *  EMPLOYEE --[WorksAtProject]--> PROJECT
   *  PROJECT --[HasManager]--> EMPLOYEE
   *
   *  Properties manually configured on edges:
   *
   *  * WorksAtProject:
   *    - updatedOn (type DATE): mandatory=F, readOnly=F, notNull=F.
   *    - propWithoutTypeField (type not present in config --> property will be dropped): mandatory=T, readOnly=F, notNull=F.
   *  * HasManager:
   *    - updatedOn (type DATE): mandatory=F.
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, PROJECT varchar(256) not null, primary key (EMP_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table PROJECT (ID  varchar(256),"
          + " TITLE varchar(256) not null, PROJECT_MANAGER varchar(256) not null, primary key (ID))";
      st.execute(foreignTableBuilding);

      // Records Inserting

      String employeeFilling =
          "insert into EMPLOYEE (EMP_ID,FIRST_NAME,LAST_NAME,PROJECT) values (" + "('E001','Joe','Black','P001'),"
              + "('E002','Thomas','Anderson','P002')," + "('E003','Tyler','Durden','P001'),"
              + "('E004','John','McClanenei','P001')," + "('E005','Ellen','Ripley','P002')," + "('E006','Marty','McFly','P002'))";
      st.execute(employeeFilling);

      String projectFilling = "insert into PROJECT (ID,TITLE,PROJECT_MANAGER) values (" + "('P001','Data Migration','E001'),"
          + "('P002','Contracts Update','E005'))";
      st.execute(projectFilling);

      ODocument config = OMigrationConfigManager.loadMigrationConfigFromFile(this.configDirectEdgesPath);

      this.naiveStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, config);


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

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      // vertices check

      assertEquals(8, orientGraph.countClass("V"));
      assertEquals(6, orientGraph.countClass("Employee"));
      assertEquals(2, orientGraph.countClass("Project"));

      // edges check

      assertEquals(8, orientGraph.countClass("E"));
      assertEquals(6, orientGraph.countClass("WorksAtProject"));
      assertEquals(2, orientGraph.countClass("HasManager"));

      // vertex properties and connections check
      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "P001" };

      OVertex v = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Project", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("P001", v.getProperty("id"));
        assertEquals("Data Migration", v.getProperty("title"));
        assertEquals("E001", v.getProperty("projectManager"));
        edgesIt = v.getEdges(ODirection.IN, "WorksAtProject").iterator();
        assertEquals("E001", edgesIt.next().getVertex(ODirection.OUT).getProperty("empId"));
        assertEquals("E003", edgesIt.next().getVertex(ODirection.OUT).getProperty("empId"));
        assertEquals("E004", edgesIt.next().getVertex(ODirection.OUT).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasManager").iterator();
        assertEquals("E001", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());

      } else {
        fail("Query fail!");
      }

      values[0] = "P002";
      result = OGraphCommands.getVertices(orientGraph, "Project", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("P002", v.getProperty("id"));
        assertEquals("Contracts Update", v.getProperty("title"));
        assertEquals("E005", v.getProperty("projectManager"));
        edgesIt = v.getEdges(ODirection.IN, "WorksAtProject").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.OUT).getProperty("empId"));
        assertEquals("E005", edgesIt.next().getVertex(ODirection.OUT).getProperty("empId"));
        assertEquals("E006", edgesIt.next().getVertex(ODirection.OUT).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasManager").iterator();
        assertEquals("E005", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      keys[0] = "empId";
      values[0] = "E001";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E001", v.getProperty("empId"));
        assertEquals("Joe", v.getProperty("firstName"));
        assertEquals("Black", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAtProject").iterator();
        assertEquals("P001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
        assertEquals("P001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E002";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E002", v.getProperty("empId"));
        assertEquals("Thomas", v.getProperty("firstName"));
        assertEquals("Anderson", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAtProject").iterator();
        assertEquals("P002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E003";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E003", v.getProperty("empId"));
        assertEquals("Tyler", v.getProperty("firstName"));
        assertEquals("Durden", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAtProject").iterator();
        assertEquals("P001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E004";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E004", v.getProperty("empId"));
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("McClanenei", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAtProject").iterator();
        assertEquals("P001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E005";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E005", v.getProperty("empId"));
        assertEquals("Ellen", v.getProperty("firstName"));
        assertEquals("Ripley", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAtProject").iterator();
        assertEquals("P002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
        assertEquals("P002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E006";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E006", v.getProperty("empId"));
        assertEquals("Marty", v.getProperty("firstName"));
        assertEquals("McFly", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.OUT, "WorksAtProject").iterator();
        assertEquals("P002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasManager").iterator();
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

        if (orientGraph != null) {
          this.context.dropOrientDBDatabase(this.dbName);
          this.context.closeOrientDBInstance();
        }

        OFileManager.deleteResource(this.outParentDirectory + this.dbName);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }
  }

  //@Test

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
   *    - updatedOn (type DATE): mandatory=F, readOnly=F, notNull=F.
   *    - propWithoutTypeField (type not present in config --> property will be dropped): mandatory=T, readOnly=F, notNull=F.
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, PROJECT varchar(256) not null, primary key (EMP_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table PROJECT (ID  varchar(256),"
          + " TITLE varchar(256) not null, PROJECT_MANAGER varchar(256) not null, primary key (ID), "
          + "foreign key (PROJECT_MANAGER) references EMPLOYEE(EMP_ID))";
      st.execute(foreignTableBuilding);

      // Records Inserting

      String employeeFilling =
          "insert into EMPLOYEE (EMP_ID,FIRST_NAME,LAST_NAME,PROJECT) values (" + "('E001','Joe','Black','P001'),"
              + "('E002','Thomas','Anderson','P002')," + "('E003','Tyler','Durden','P001'),"
              + "('E004','John','McClanenei','P001')," + "('E005','Ellen','Ripley','P002')," + "('E006','Marty','McFly','P002'))";
      st.execute(employeeFilling);

      String projectFilling = "insert into PROJECT (ID,TITLE,PROJECT_MANAGER) values (" + "('P001','Data Migration','E001'),"
          + "('P002','Contracts Update','E005'))";
      st.execute(projectFilling);

      parentTableBuilding = "alter table EMPLOYEE add foreign key (PROJECT) references PROJECT(ID)";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      ODocument config = OMigrationConfigManager.loadMigrationConfigFromFile(this.configInverseEdgesPath);

      this.naiveStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, config);


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

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      // vertices check

      assertEquals(8, orientGraph.countClass("V"));
      assertEquals(6, orientGraph.countClass("Employee"));
      assertEquals(2, orientGraph.countClass("Project"));

      // edges check

      assertEquals(8, orientGraph.countClass("E"));
      assertEquals(6, orientGraph.countClass("HasEmployee"));
      assertEquals(2, orientGraph.countClass("HasProjectManager"));

      // vertex properties and connections check
      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "P001" };

      OVertex v = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Project", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("P001", v.getProperty("id"));
        assertEquals("Data Migration", v.getProperty("title"));
        assertEquals("E001", v.getProperty("projectManager"));
        edgesIt = v.getEdges(ODirection.OUT, "HasEmployee").iterator();
        assertEquals("E001", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals("E003", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals("E004", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasProjectManager").iterator();
        assertEquals("E001", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());

      } else {
        fail("Query fail!");
      }

      values[0] = "P002";
      result = OGraphCommands.getVertices(orientGraph, "Project", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("P002", v.getProperty("id"));
        assertEquals("Contracts Update", v.getProperty("title"));
        assertEquals("E005", v.getProperty("projectManager"));
        edgesIt = v.getEdges(ODirection.OUT, "HasEmployee").iterator();
        assertEquals("E002", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals("E005", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals("E006", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.OUT, "HasProjectManager").iterator();
        assertEquals("E005", edgesIt.next().getVertex(ODirection.IN).getProperty("empId"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      keys[0] = "empId";
      values[0] = "E001";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E001", v.getProperty("empId"));
        assertEquals("Joe", v.getProperty("firstName"));
        assertEquals("Black", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.IN, "HasEmployee").iterator();
        assertEquals("P001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasProjectManager").iterator();
        assertEquals("P001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E002";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E002", v.getProperty("empId"));
        assertEquals("Thomas", v.getProperty("firstName"));
        assertEquals("Anderson", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.IN, "HasEmployee").iterator();
        assertEquals("P002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasProjectManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E003";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E003", v.getProperty("empId"));
        assertEquals("Tyler", v.getProperty("firstName"));
        assertEquals("Durden", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.IN, "HasEmployee").iterator();
        assertEquals("P001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasProjectManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E004";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E004", v.getProperty("empId"));
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("McClanenei", v.getProperty("lastName"));
        assertEquals("P001", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.IN, "HasEmployee").iterator();
        assertEquals("P001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasProjectManager").iterator();
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E005";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E005", v.getProperty("empId"));
        assertEquals("Ellen", v.getProperty("firstName"));
        assertEquals("Ripley", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.IN, "HasEmployee").iterator();
        assertEquals("P002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasProjectManager").iterator();
        assertEquals("P002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "E006";
      result = OGraphCommands.getVertices(orientGraph, "Employee", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("E006", v.getProperty("empId"));
        assertEquals("Marty", v.getProperty("firstName"));
        assertEquals("McFly", v.getProperty("lastName"));
        assertEquals("P002", v.getProperty("project"));
        edgesIt = v.getEdges(ODirection.IN, "HasEmployee").iterator();
        assertEquals("P002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
        edgesIt = v.getEdges(ODirection.IN, "HasProjectManager").iterator();
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

        if (orientGraph != null) {
          this.context.dropOrientDBDatabase(this.dbName);
          this.context.closeOrientDBInstance();
        }

        OFileManager.deleteResource(this.outParentDirectory + this.dbName);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }
  }

  //@Test

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
   *    - payment (type INTEGER): mandatory=T, readOnly=F, notNull=F.
   *    - year (type DATE): mandatory=F, readOnly=F, notNull=F.
   */

  public void test3() {

    this.context.setExecutionStrategy("naive-aggregate");
    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table ACTOR (ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding =
          "create memory table FILM (ID varchar(256)," + " TITLE varchar(256) not null, CATEGORY varchar(256), primary key (ID))";
      st.execute(foreignTableBuilding);

      String actorFilmTableBuilding = "create memory table ACTOR_FILM (ACTOR_ID  varchar(256),"
          + " FILM_ID varchar(256) not null, PAYMENT integer, primary key (ACTOR_ID, FILM_ID))";
      st.execute(actorFilmTableBuilding);

      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,CATEGORY) values (" + "('F001','Pulp Fiction','Action'),"
          + "('F002','Shutter Island','Thriller')," + "('F003','The Departed','Action-Thriller'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,FIRST_NAME,LAST_NAME) values (" + "('A001','John','Travolta'),"
          + "('A002','Samuel','Lee Jackson')," + "('A003','Bruce','Willis')," + "('A004','Leonardo','Di Caprio'),"
          + "('A005','Ben','Kingsley')," + "('A006','Mark','Ruffalo')," + "('A007','Jack','Nicholson'),"
          + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into ACTOR_FILM (ACTOR_ID,FILM_ID,PAYMENT) values (" + "('A001','F001','12000000'),"
          + "('A002','F001','10000000')," + "('A003','F001','15000000')," + "('A004','F002','30000000'),"
          + "('A004','F003','40000000')," + "('A005','F002','35000000')," + "('A006','F002','9000000'),"
          + "('A007','F003','25000000')," + "('A008','F003','15000000'))";
      st.execute(film2actorFilling);

      ODocument config = OMigrationConfigManager.loadMigrationConfigFromFile(this.configJoinTableDirectEdgesPath);

      this.naiveAggregationStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, config);

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

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      // vertices check

      assertEquals(11, orientGraph.countClass("V"));
      assertEquals(3, orientGraph.countClass("Film"));
      assertEquals(8, orientGraph.countClass("Actor"));

      // edges check
      assertEquals(9, orientGraph.countClass("E"));
      assertEquals(9, orientGraph.countClass("Performs"));

      // vertex properties and connections check
      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "F001" };

      OVertex v = null;
      OEdge currentEdge = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("Pulp Fiction", v.getProperty("title"));
        assertEquals("Action", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(12000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(10000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F002";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F002", v.getProperty("id"));
        assertEquals("Shutter Island", v.getProperty("title"));
        assertEquals("Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(30000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(35000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(9000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F003";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F003", v.getProperty("id"));
        assertEquals("The Departed", v.getProperty("title"));
        assertEquals("Action-Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(40000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(25000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A008", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A001";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A001", v.getProperty("id"));
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("Travolta", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(12000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A002";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A002", v.getProperty("id"));
        assertEquals("Samuel", v.getProperty("firstName"));
        assertEquals("Lee Jackson", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(10000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A003";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A003", v.getProperty("id"));
        assertEquals("Bruce", v.getProperty("firstName"));
        assertEquals("Willis", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A004";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A004", v.getProperty("id"));
        assertEquals("Leonardo", v.getProperty("firstName"));
        assertEquals("Di Caprio", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(30000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(40000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A005";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A005", v.getProperty("id"));
        assertEquals("Ben", v.getProperty("firstName"));
        assertEquals("Kingsley", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(35000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A006";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A006", v.getProperty("id"));
        assertEquals("Mark", v.getProperty("firstName"));
        assertEquals("Ruffalo", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(9000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A007";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A007", v.getProperty("id"));
        assertEquals("Jack", v.getProperty("firstName"));
        assertEquals("Nicholson", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(25000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A008";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A008", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("firstName"));
        assertEquals("Damon", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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

        if (orientGraph != null) {
          this.context.dropOrientDBDatabase(this.dbName);
          this.context.closeOrientDBInstance();
        }

        OFileManager.deleteResource(this.outParentDirectory + this.dbName);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }
  }

  //@Test

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
   *    - payment (type INTEGER): mandatory=T, readOnly=F, notNull=F.
   *    - year (type DATE): mandatory=F, readOnly=F, notNull=F.
   */

  public void test4() {

    this.context.setExecutionStrategy("naive-aggregate");
    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table ACTOR (ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding =
          "create memory table FILM (ID varchar(256)," + " TITLE varchar(256) not null, CATEGORY varchar(256), primary key (ID))";
      st.execute(foreignTableBuilding);

      String actorFilmTableBuilding = "create memory table FILM_ACTOR (FILM_ID  varchar(256),"
          + " ACTOR_ID varchar(256) not null, PAYMENT integer, primary key (FILM_ID, ACTOR_ID))";
      st.execute(actorFilmTableBuilding);

      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,CATEGORY) values (" + "('F001','Pulp Fiction','Action'),"
          + "('F002','Shutter Island','Thriller')," + "('F003','The Departed','Action-Thriller'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,FIRST_NAME,LAST_NAME) values (" + "('A001','John','Travolta'),"
          + "('A002','Samuel','Lee Jackson')," + "('A003','Bruce','Willis')," + "('A004','Leonardo','Di Caprio'),"
          + "('A005','Ben','Kingsley')," + "('A006','Mark','Ruffalo')," + "('A007','Jack','Nicholson'),"
          + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into FILM_ACTOR (FILM_ID,ACTOR_ID,PAYMENT) values (" + "('F001','A001','12000000'),"
          + "('F001','A002','10000000')," + "('F001','A003','15000000')," + "('F002','A004','30000000'),"
          + "('F002','A005','35000000')," + "('F002','A006','9000000')," + "('F003','A004','40000000'),"
          + "('F003','A007','25000000')," + "('F003','A008','15000000'))";
      st.execute(film2actorFilling);

      ODocument config = OMigrationConfigManager.loadMigrationConfigFromFile(this.configJoinTableInverseEdgesPath);

      this.naiveAggregationStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, config);

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

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      // vertices check

      assertEquals(11, orientGraph.countClass("V"));

      assertEquals(3, orientGraph.countClass("Film"));
      assertEquals(8, orientGraph.countClass("Actor"));

      // edges check
      assertEquals(9, orientGraph.countClass("E"));
      assertEquals(9, orientGraph.countClass("Performs"));

      // vertex properties and connections check
      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "F001" };

      OVertex v = null;
      OEdge currentEdge = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("Pulp Fiction", v.getProperty("title"));
        assertEquals("Action", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(12000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(10000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F002";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F002", v.getProperty("id"));
        assertEquals("Shutter Island", v.getProperty("title"));
        assertEquals("Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(30000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(35000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(9000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F003";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F003", v.getProperty("id"));
        assertEquals("The Departed", v.getProperty("title"));
        assertEquals("Action-Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(40000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(25000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A008", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A001";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A001", v.getProperty("id"));
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("Travolta", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(12000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A002";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A002", v.getProperty("id"));
        assertEquals("Samuel", v.getProperty("firstName"));
        assertEquals("Lee Jackson", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(10000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A003";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A003", v.getProperty("id"));
        assertEquals("Bruce", v.getProperty("firstName"));
        assertEquals("Willis", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A004";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A004", v.getProperty("id"));
        assertEquals("Leonardo", v.getProperty("firstName"));
        assertEquals("Di Caprio", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(30000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(40000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A005";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A005", v.getProperty("id"));
        assertEquals("Ben", v.getProperty("firstName"));
        assertEquals("Kingsley", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(35000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A006";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A006", v.getProperty("id"));
        assertEquals("Mark", v.getProperty("firstName"));
        assertEquals("Ruffalo", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(9000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A007";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A007", v.getProperty("id"));
        assertEquals("Jack", v.getProperty("firstName"));
        assertEquals("Nicholson", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(25000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A008";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A008", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("firstName"));
        assertEquals("Damon", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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

        if (orientGraph != null) {
          this.context.dropOrientDBDatabase(this.dbName);
          this.context.closeOrientDBInstance();
        }

        OFileManager.deleteResource(this.outParentDirectory + this.dbName);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }
  }

  //@Test

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
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table ACTOR (ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding =
          "create memory table FILM (ID varchar(256)," + " TITLE varchar(256) not null, CATEGORY varchar(256), primary key (ID))";
      st.execute(foreignTableBuilding);

      String actorFilmTableBuilding = "create memory table ACTOR_FILM (ACTOR_ID  varchar(256),"
          + " FILM_ID varchar(256) not null, PAYMENT integer, primary key (ACTOR_ID, FILM_ID),"
          + " foreign key (ACTOR_ID) references ACTOR(ID), foreign key (FILM_ID) references FILM(ID))";
      st.execute(actorFilmTableBuilding);

      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,CATEGORY) values (" + "('F001','Pulp Fiction','Action'),"
          + "('F002','Shutter Island','Thriller')," + "('F003','The Departed','Action-Thriller'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,FIRST_NAME,LAST_NAME) values (" + "('A001','John','Travolta'),"
          + "('A002','Samuel','Lee Jackson')," + "('A003','Bruce','Willis')," + "('A004','Leonardo','Di Caprio'),"
          + "('A005','Ben','Kingsley')," + "('A006','Mark','Ruffalo')," + "('A007','Jack','Nicholson'),"
          + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into ACTOR_FILM (ACTOR_ID,FILM_ID,PAYMENT) values (" + "('A001','F001','12000000'),"
          + "('A002','F001','10000000')," + "('A003','F001','15000000')," + "('A004','F002','30000000'),"
          + "('A004','F003','40000000')," + "('A005','F002','35000000')," + "('A006','F002','9000000'),"
          + "('A007','F003','25000000')," + "('A008','F003','15000000'))";
      st.execute(film2actorFilling);

      ODocument config = OMigrationConfigManager.loadMigrationConfigFromFile(this.configJoinTableDirectEdgesPath);

      this.naiveAggregationStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, config);

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

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      // vertices check

      assertEquals(11, orientGraph.countClass("V"));
      assertEquals(3, orientGraph.countClass("Film"));
      assertEquals(8, orientGraph.countClass("Actor"));

      // edges check
      assertEquals(9, orientGraph.countClass("E"));
      assertEquals(9, orientGraph.countClass("Performs"));

      // vertex properties and connections check
      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "F001" };

      OVertex v = null;
      OEdge currentEdge = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("Pulp Fiction", v.getProperty("title"));
        assertEquals("Action", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(12000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(10000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F002";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F002", v.getProperty("id"));
        assertEquals("Shutter Island", v.getProperty("title"));
        assertEquals("Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(30000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(35000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(9000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F003";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F003", v.getProperty("id"));
        assertEquals("The Departed", v.getProperty("title"));
        assertEquals("Action-Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(40000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(25000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A008", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A001";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A001", v.getProperty("id"));
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("Travolta", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(12000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A002";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A002", v.getProperty("id"));
        assertEquals("Samuel", v.getProperty("firstName"));
        assertEquals("Lee Jackson", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(10000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A003";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A003", v.getProperty("id"));
        assertEquals("Bruce", v.getProperty("firstName"));
        assertEquals("Willis", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A004";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A004", v.getProperty("id"));
        assertEquals("Leonardo", v.getProperty("firstName"));
        assertEquals("Di Caprio", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(30000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(40000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A005";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A005", v.getProperty("id"));
        assertEquals("Ben", v.getProperty("firstName"));
        assertEquals("Kingsley", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(35000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A006";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A006", v.getProperty("id"));
        assertEquals("Mark", v.getProperty("firstName"));
        assertEquals("Ruffalo", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(9000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A007";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A007", v.getProperty("id"));
        assertEquals("Jack", v.getProperty("firstName"));
        assertEquals("Nicholson", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(25000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A008";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A008", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("firstName"));
        assertEquals("Damon", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.OUT, "Performs").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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

        if (orientGraph != null) {
          this.context.dropOrientDBDatabase(this.dbName);
          this.context.closeOrientDBInstance();
        }

        OFileManager.deleteResource(this.outParentDirectory + this.dbName);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }
  }

  //@Test

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
   *  ACTOR --[Performs]--> FILM
   *
   *  But with the "inverse" direction we obtain:
   *
   *  FILM --[Features]--> ACTOR
   *
   *  Features:
   *    - year (type DATE): mandatory=T, readOnly=F, notNull=F.
   */

  public void test6() {

    this.context.setExecutionStrategy("naive-aggregate");
    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table ACTOR (ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding =
          "create memory table FILM (ID varchar(256)," + " TITLE varchar(256) not null, CATEGORY varchar(256), primary key (ID))";
      st.execute(foreignTableBuilding);

      String actorFilmTableBuilding = "create memory table FILM_ACTOR (ACTOR_ID  varchar(256),"
          + " FILM_ID varchar(256) not null, PAYMENT integer, primary key (ACTOR_ID, FILM_ID),"
          + " foreign key (ACTOR_ID) references ACTOR(ID), foreign key (FILM_ID) references FILM(ID))";
      st.execute(actorFilmTableBuilding);

      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,CATEGORY) values (" + "('F001','Pulp Fiction','Action'),"
          + "('F002','Shutter Island','Thriller')," + "('F003','The Departed','Action-Thriller'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,FIRST_NAME,LAST_NAME) values (" + "('A001','John','Travolta'),"
          + "('A002','Samuel','Lee Jackson')," + "('A003','Bruce','Willis')," + "('A004','Leonardo','Di Caprio'),"
          + "('A005','Ben','Kingsley')," + "('A006','Mark','Ruffalo')," + "('A007','Jack','Nicholson'),"
          + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into FILM_ACTOR (FILM_ID,ACTOR_ID,PAYMENT) values (" + "('F001','A001','12000000'),"
          + "('F001','A002','10000000')," + "('F001','A003','15000000')," + "('F002','A004','30000000'),"
          + "('F002','A005','35000000')," + "('F002','A006','9000000')," + "('F003','A004','40000000'),"
          + "('F003','A007','25000000')," + "('F003','A008','15000000'))";
      st.execute(film2actorFilling);

      ODocument config = OMigrationConfigManager.loadMigrationConfigFromFile(this.configJoinTableInverseEdgesPath2);

      this.naiveAggregationStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, config);

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

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      // vertices check

      assertEquals(11, orientGraph.countClass("V"));
      assertEquals(3, orientGraph.countClass("Film"));
      assertEquals(8, orientGraph.countClass("Actor"));

      // edges check

      assertEquals(9, orientGraph.countClass("E"));
      assertEquals(9, orientGraph.countClass("Features"));

      // vertex properties and connections check
      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "F001" };

      OVertex v = null;
      OEdge currentEdge = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("Pulp Fiction", v.getProperty("title"));
        assertEquals("Action", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.OUT, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(12000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(10000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F002";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F002", v.getProperty("id"));
        assertEquals("Shutter Island", v.getProperty("title"));
        assertEquals("Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.OUT, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(30000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(35000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(9000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F003";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F003", v.getProperty("id"));
        assertEquals("The Departed", v.getProperty("title"));
        assertEquals("Action-Thriller", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.OUT, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(40000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(25000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("A008", currentEdge.getVertex(ODirection.IN).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A001";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A001", v.getProperty("id"));
        assertEquals("John", v.getProperty("firstName"));
        assertEquals("Travolta", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(12000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A002";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A002", v.getProperty("id"));
        assertEquals("Samuel", v.getProperty("firstName"));
        assertEquals("Lee Jackson", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(10000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A003";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A003", v.getProperty("id"));
        assertEquals("Bruce", v.getProperty("firstName"));
        assertEquals("Willis", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A004";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A004", v.getProperty("id"));
        assertEquals("Leonardo", v.getProperty("firstName"));
        assertEquals("Di Caprio", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(30000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(Integer.valueOf(40000000), currentEdge.getProperty("payment"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A005";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A005", v.getProperty("id"));
        assertEquals("Ben", v.getProperty("firstName"));
        assertEquals("Kingsley", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(35000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A006";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A006", v.getProperty("id"));
        assertEquals("Mark", v.getProperty("firstName"));
        assertEquals("Ruffalo", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F002", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(9000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A007";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A007", v.getProperty("id"));
        assertEquals("Jack", v.getProperty("firstName"));
        assertEquals("Nicholson", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(25000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A008";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A008", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("firstName"));
        assertEquals("Damon", v.getProperty("lastName"));
        edgesIt = v.getEdges(ODirection.IN, "Features").iterator();
        currentEdge = edgesIt.next();
        assertEquals("F003", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(15000000), currentEdge.getProperty("payment"));
        assertNull(currentEdge.getProperty("year"));
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

        if (orientGraph != null) {
          this.context.dropOrientDBDatabase(this.dbName);
          this.context.closeOrientDBInstance();
        }

        OFileManager.deleteResource(this.outParentDirectory + this.dbName);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }
  }

}
