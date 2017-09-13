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

package com.orientechnologies.teleporter.test.rdbms.configuration.orientWriter;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.util.OFileManager;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */

public class OrientDBSchemaWritingWithAggregationTest {

  private OER2GraphMapper    mapper;
  private OTeleporterContext context;
  private OGraphModelWriter  modelWriter;
  private final String config = "src/test/resources/configuration-mapping/aggregation-from2tables-mapping.json";
  private ODBQueryEngine dbQueryEngine;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private String dbName = "testOrientDB";
  private String protocol = "plocal";
  private String outParentDirectory = "embedded:target/";
  private String outOrientGraphUri = this.outParentDirectory + this.dbName;
  private OSourceDatabaseInfo sourceDBInfo;

  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance(this.outParentDirectory);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setMessageHandler(new OTeleporterMessageHandler(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.modelWriter = new OGraphModelWriter();
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
   *  - 1 mysql source
   *  - 1 relationship from person to department (not declared through foreign key definition)
   *  - 3 tables: "person", "vat_profile", "department"
   *
   *  person(id, name, surname, dep_id)
   *  vat_profile(id, vat, updated_on)
   *  department(id, name, location, updated_on)
   *
   *  Desired Graph Model:
   *
   *  - 2 vertex classes: "Person" (aggregation of person and vat_profile entities) and "Department"
   *  - 1 edge class "WorksAt", corresponding to the logic relationship between "person" and "department"
   *
   *  Person(extKey1, extKey2, firstName, lastName, VAT)
   *  Department(id, departmentName, location)
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String personTableBuilding = "create memory table PERSON (ID varchar(256) not null,"
          + " NAME varchar(256) not null, SURNAME varchar(256) not null, DEP_ID varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(personTableBuilding);

      String vatProfileTableBuilding = "create memory table VAT_PROFILE (ID varchar(256),"
          + " VAT varchar(256) not null, UPDATED_ON date not null, primary key (ID))";
      st.execute(vatProfileTableBuilding);

      String departmentTableBuilding = "create memory table DEPARTMENT (ID  varchar(256),"
          + " NAME varchar(256) not null, LOCATION varchar(256) not null, UPDATED_ON date not null, primary key (ID))";
      st.execute(departmentTableBuilding);

      ODocument config = OFileManager.buildJsonFromFile(this.config);
      OConfigurationHandler configHandler = new OConfigurationHandler(true);
      OConfiguration migrationConfig = configHandler.buildConfigurationFromJSONDoc(config);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, migrationConfig);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());
      mapper.applyImportConfiguration();
      modelWriter.writeModelOnOrient(mapper, new OHSQLDBDataTypeHandler(), this.dbName, this.protocol);

      /**
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfVertexTypes);
      assertEquals(2, context.getStatistics().wroteVertexType);
      assertEquals(1, context.getStatistics().totalNumberOfEdgeTypes);
      assertEquals(1, context.getStatistics().wroteEdgeType);
      assertEquals(2, context.getStatistics().totalNumberOfIndices);
      assertEquals(2, context.getStatistics().wroteIndexes);

      /**
       *  Testing built OrientDB schema
       */


      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      OClass personVertexType = orientGraph.getClass("Person");
      OClass departmentVertexType = orientGraph.getClass("Department");
      OClass worksAtEdgeType = orientGraph.getClass("WorksAt");

      // vertices check

      assertNotNull(personVertexType);
      assertNotNull(departmentVertexType);

      // properties check

      assertNotNull(personVertexType.getProperty("extKey1"));
      assertEquals("extKey1", personVertexType.getProperty("extKey1").getName());
      assertEquals(OType.STRING, personVertexType.getProperty("extKey1").getType());
      assertEquals(false, personVertexType.getProperty("extKey1").isMandatory());
      assertEquals(false, personVertexType.getProperty("extKey1").isReadonly());
      assertEquals(false, personVertexType.getProperty("extKey1").isNotNull());

      assertNotNull(personVertexType.getProperty("firstName"));
      assertEquals("firstName", personVertexType.getProperty("firstName").getName());
      assertEquals(OType.STRING, personVertexType.getProperty("firstName").getType());
      assertEquals(true, personVertexType.getProperty("firstName").isMandatory());
      assertEquals(false, personVertexType.getProperty("firstName").isReadonly());
      assertEquals(true, personVertexType.getProperty("firstName").isNotNull());

      assertNotNull(personVertexType.getProperty("lastName"));
      assertEquals("lastName", personVertexType.getProperty("lastName").getName());
      assertEquals(OType.STRING, personVertexType.getProperty("lastName").getType());
      assertEquals(true, personVertexType.getProperty("lastName").isMandatory());
      assertEquals(false, personVertexType.getProperty("lastName").isReadonly());
      assertEquals(true, personVertexType.getProperty("lastName").isNotNull());

      assertNull(personVertexType.getProperty("depId"));

      assertNotNull(personVertexType.getProperty("extKey2"));
      assertEquals("extKey2", personVertexType.getProperty("extKey2").getName());
      assertEquals(OType.STRING, personVertexType.getProperty("extKey2").getType());
      assertEquals(false, personVertexType.getProperty("extKey2").isMandatory());
      assertEquals(false, personVertexType.getProperty("extKey2").isReadonly());
      assertEquals(false, personVertexType.getProperty("extKey2").isNotNull());

      assertNotNull(personVertexType.getProperty("VAT"));
      assertEquals("VAT", personVertexType.getProperty("VAT").getName());
      assertEquals(OType.STRING, personVertexType.getProperty("VAT").getType());
      assertEquals(true, personVertexType.getProperty("VAT").isMandatory());
      assertEquals(false, personVertexType.getProperty("VAT").isReadonly());
      assertEquals(true, personVertexType.getProperty("VAT").isNotNull());

      assertNull(personVertexType.getProperty("updatedOn"));

      assertNotNull(departmentVertexType.getProperty("id"));
      assertEquals("id", departmentVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, departmentVertexType.getProperty("id").getType());
      assertEquals(false, departmentVertexType.getProperty("id").isMandatory());
      assertEquals(false, departmentVertexType.getProperty("id").isReadonly());
      assertEquals(false, departmentVertexType.getProperty("id").isNotNull());

      assertNotNull(departmentVertexType.getProperty("departmentName"));
      assertEquals("departmentName", departmentVertexType.getProperty("departmentName").getName());
      assertEquals(OType.STRING, departmentVertexType.getProperty("departmentName").getType());
      assertEquals(false, departmentVertexType.getProperty("departmentName").isMandatory());
      assertEquals(false, departmentVertexType.getProperty("departmentName").isReadonly());
      assertEquals(true, departmentVertexType.getProperty("departmentName").isNotNull());

      assertNotNull(departmentVertexType.getProperty("location"));
      assertEquals("location", departmentVertexType.getProperty("location").getName());
      assertEquals(OType.STRING, departmentVertexType.getProperty("location").getType());
      assertEquals(false, departmentVertexType.getProperty("location").isMandatory());
      assertEquals(false, departmentVertexType.getProperty("location").isReadonly());
      assertEquals(true, departmentVertexType.getProperty("location").isNotNull());

      assertNull(departmentVertexType.getProperty("updatedOn"));

      // edges check
      assertNotNull(worksAtEdgeType);

      assertEquals("WorksAt", worksAtEdgeType.getName());
      assertEquals(1, worksAtEdgeType.propertiesMap().size());

      assertEquals("since", worksAtEdgeType.getProperty("since").getName());
      assertEquals(OType.DATE, worksAtEdgeType.getProperty("since").getType());
      assertEquals(false, worksAtEdgeType.getProperty("since").isMandatory());
      assertEquals(false, worksAtEdgeType.getProperty("since").isReadonly());
      assertEquals(false, worksAtEdgeType.getProperty("since").isNotNull());

      // Indices check
      assertEquals(true, orientGraph.getMetadata().getIndexManager().existsIndex("Person.pkey"));
      assertEquals(true, orientGraph.getMetadata().getIndexManager().areIndexed("Person", "extKey1", "extKey2"));

      assertEquals(true, orientGraph.getMetadata().getIndexManager().existsIndex("Department.pkey"));
      assertEquals(true, orientGraph.getMetadata().getIndexManager().areIndexed("Department", "id"));

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
