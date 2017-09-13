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
import static org.junit.Assert.fail;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OrientDBSchemaWritingWithSplittingTest {

  private OER2GraphMapper    mapper;
  private OTeleporterContext context;
  private OGraphModelWriter  modelWriter;
  private final String config = "src/test/resources/configuration-mapping/splitting-into2tables-mapping.json";
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
   *  - 1 hsqldb source
   *  - 1 relationship from employee to department (not declared through foreign key definition)
   *  - 2 tables: "employee", "department"
   *
   *  employee(first_name, last_name, salary, department, project, balance, role)
   *  department(id, name, location, updated_on)
   *
   *  Desired Graph Model:
   *
   *  - 3 vertex classes: "Employee" and "Project" (both split from employee entity), "Department"
   *  - 1 edge class "WorksAt", corresponding to the relationship between "person" and "department"
   *  - 1 edge class "HasProject", representing the splitting-edge connecting each couple of instances of "Employee"
   *    and "Project" coming from the same record of the "employee" table. It has a "role" property coming from the
   *    "employee" table too.
   *
   *  Employee(firstName, lastName, salary, department)
   *  Project(project, balance, role)
   *  Department(id, departmentName, location)
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

      assertEquals(4, context.getStatistics().totalNumberOfVertexTypes);
      assertEquals(4, context.getStatistics().wroteVertexType);
      assertEquals(3, context.getStatistics().totalNumberOfEdgeTypes);
      assertEquals(3, context.getStatistics().wroteEdgeType);
      assertEquals(4, context.getStatistics().totalNumberOfIndices);
      assertEquals(4, context.getStatistics().wroteIndexes);

      /**
       *  Testing built OrientDB schema
       */


      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      OClass employeeVertexType = orientGraph.getClass("Employee");
      OClass projectVertexType = orientGraph.getClass("Project");
      OClass departmentVertexType = orientGraph.getClass("Department");
      OClass chiefOfficerVertexType = orientGraph.getClass("ChiefOfficer");
      OClass worksAtEdgeType = orientGraph.getClass("WorksAt");
      OClass hasProjectEdgeType = orientGraph.getClass("HasProject");
      OClass isChiefForProjectEdgeType = orientGraph.getClass("IsChiefForProject");

      // vertices check
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);
      assertNotNull(departmentVertexType);
      assertNotNull(isChiefForProjectEdgeType);

      // properties check

      assertNotNull(employeeVertexType.getProperty("firstName"));
      assertEquals("firstName", employeeVertexType.getProperty("firstName").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("firstName").getType());
      assertEquals(false, employeeVertexType.getProperty("firstName").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("firstName").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("firstName").isNotNull());

      assertNotNull(employeeVertexType.getProperty("lastName"));
      assertEquals("lastName", employeeVertexType.getProperty("lastName").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("lastName").getType());
      assertEquals(false, employeeVertexType.getProperty("lastName").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("lastName").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("lastName").isNotNull());

      assertNotNull(employeeVertexType.getProperty("salary"));
      assertEquals("salary", employeeVertexType.getProperty("salary").getName());
      assertEquals(OType.DECIMAL, employeeVertexType.getProperty("salary").getType());
      assertEquals(false, employeeVertexType.getProperty("salary").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("salary").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("salary").isNotNull());

      assertNotNull(employeeVertexType.getProperty("department"));
      assertEquals("department", employeeVertexType.getProperty("department").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("department").getType());
      assertEquals(false, employeeVertexType.getProperty("department").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("department").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("department").isNotNull());

      assertNotNull(projectVertexType.getProperty("project"));
      assertEquals("project", projectVertexType.getProperty("project").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("project").getType());
      assertEquals(false, projectVertexType.getProperty("project").isMandatory());
      assertEquals(false, projectVertexType.getProperty("project").isReadonly());
      assertEquals(false, projectVertexType.getProperty("project").isNotNull());

      assertNotNull(projectVertexType.getProperty("balance"));
      assertEquals("balance", projectVertexType.getProperty("balance").getName());
      assertEquals(OType.DECIMAL, projectVertexType.getProperty("balance").getType());
      assertEquals(false, projectVertexType.getProperty("balance").isMandatory());
      assertEquals(false, projectVertexType.getProperty("balance").isReadonly());
      assertEquals(false, projectVertexType.getProperty("balance").isNotNull());

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

      assertNotNull(chiefOfficerVertexType.getProperty("firstName"));
      assertEquals("firstName", chiefOfficerVertexType.getProperty("firstName").getName());
      assertEquals(OType.STRING, chiefOfficerVertexType.getProperty("firstName").getType());
      assertEquals(true, chiefOfficerVertexType.getProperty("firstName").isMandatory());
      assertEquals(false, chiefOfficerVertexType.getProperty("firstName").isReadonly());
      assertEquals(true, chiefOfficerVertexType.getProperty("firstName").isNotNull());

      assertNotNull(chiefOfficerVertexType.getProperty("lastName"));
      assertEquals("lastName", chiefOfficerVertexType.getProperty("lastName").getName());
      assertEquals(OType.STRING, chiefOfficerVertexType.getProperty("lastName").getType());
      assertEquals(true, chiefOfficerVertexType.getProperty("lastName").isMandatory());
      assertEquals(false, chiefOfficerVertexType.getProperty("lastName").isReadonly());
      assertEquals(true, chiefOfficerVertexType.getProperty("lastName").isNotNull());

      assertNotNull(chiefOfficerVertexType.getProperty("project"));
      assertEquals("project", chiefOfficerVertexType.getProperty("project").getName());
      assertEquals(OType.STRING, chiefOfficerVertexType.getProperty("project").getType());
      assertEquals(false, chiefOfficerVertexType.getProperty("project").isMandatory());
      assertEquals(false, chiefOfficerVertexType.getProperty("project").isReadonly());
      assertEquals(false, chiefOfficerVertexType.getProperty("project").isNotNull());

      // edges check
      assertNotNull(worksAtEdgeType);
      assertNotNull(hasProjectEdgeType);
      assertNotNull(isChiefForProjectEdgeType);

      assertEquals("WorksAt", worksAtEdgeType.getName());
      assertEquals(1, worksAtEdgeType.propertiesMap().size());

      assertEquals("since", worksAtEdgeType.getProperty("since").getName());
      assertEquals(OType.DATE, worksAtEdgeType.getProperty("since").getType());
      assertEquals(false, worksAtEdgeType.getProperty("since").isMandatory());
      assertEquals(false, worksAtEdgeType.getProperty("since").isReadonly());
      assertEquals(false, worksAtEdgeType.getProperty("since").isNotNull());

      assertEquals("HasProject", hasProjectEdgeType.getName());
      assertEquals(1, hasProjectEdgeType.propertiesMap().size());

      assertEquals("role", hasProjectEdgeType.getProperty("role").getName());
      assertEquals(OType.STRING, hasProjectEdgeType.getProperty("role").getType());
      assertEquals(false, hasProjectEdgeType.getProperty("role").isMandatory());
      assertEquals(false, hasProjectEdgeType.getProperty("role").isReadonly());
      assertEquals(false, hasProjectEdgeType.getProperty("role").isNotNull());

      assertEquals("IsChiefForProject", isChiefForProjectEdgeType.getName());
      assertEquals(0, isChiefForProjectEdgeType.propertiesMap().size());

      // Indices check
      assertEquals(true, orientGraph.getMetadata().getIndexManager().existsIndex("Employee.pkey"));
      assertEquals(true, orientGraph.getMetadata().getIndexManager().areIndexed("Employee", "firstName", "lastName"));

      assertEquals(true, orientGraph.getMetadata().getIndexManager().existsIndex("Project.pkey"));
      assertEquals(true, orientGraph.getMetadata().getIndexManager().areIndexed("Project", "project"));

      assertEquals(true, orientGraph.getMetadata().getIndexManager().existsIndex("Department.pkey"));
      assertEquals(true, orientGraph.getMetadata().getIndexManager().areIndexed("Department", "id"));

      assertEquals(true, orientGraph.getMetadata().getIndexManager().existsIndex("ChiefOfficer.pkey"));
      assertEquals(true, orientGraph.getMetadata().getIndexManager().areIndexed("ChiefOfficer", "firstName", "lastName"));

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
