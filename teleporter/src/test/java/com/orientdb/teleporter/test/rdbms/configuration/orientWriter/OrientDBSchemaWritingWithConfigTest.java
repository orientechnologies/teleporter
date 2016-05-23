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

package com.orientdb.teleporter.test.rdbms.configuration.orientWriter;

import com.orientdb.teleporter.context.OOutputStreamManager;
import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientdb.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientdb.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientdb.teleporter.util.OFileManager;
import com.orientdb.teleporter.writer.OGraphModelWriter;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class OrientDBSchemaWritingWithConfigTest {

  private OER2GraphMapper    mapper;
  private OTeleporterContext context;
  private OGraphModelWriter  modelWriter;
  private String             outOrientGraphUri;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setQueryQuoteType("\"");
    this.modelWriter = new OGraphModelWriter();
    this.outOrientGraphUri = "memory:testOrientDB";
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

      ODocument config = OFileManager.buildJsonFromFile("src/test/resources/configuration-mapping/relationships-mapping-direct-edges.json");

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null, config);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfVertexType);
      assertEquals(2, context.getStatistics().wroteVertexType);
      assertEquals(1, context.getStatistics().totalNumberOfEdgeType);
      assertEquals(1, context.getStatistics().wroteEdgeType);
      assertEquals(2, context.getStatistics().totalNumberOfIndices);
      assertEquals(2, context.getStatistics().wroteIndexes);

      /*
       *  Testing built OrientDB schema
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      OrientVertexType employeeVertexType =  orientGraph.getVertexType("Employee");
      OrientVertexType projectVertexType = orientGraph.getVertexType("Project");
      OrientEdgeType worksAtProjectEdgeType = orientGraph.getEdgeType("WorksAtProject");
      OrientEdgeType hasManagerEdgeType = orientGraph.getEdgeType("HasManager");

      // vertices check
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);

      // properties check
      assertNotNull(employeeVertexType.getProperty("empId"));
      assertEquals("empId", employeeVertexType.getProperty("empId").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("empId").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(employeeVertexType.getProperty("firstName"));
      assertEquals("firstName", employeeVertexType.getProperty("firstName").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("firstName").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(employeeVertexType.getProperty("lastName"));
      assertEquals("lastName", employeeVertexType.getProperty("lastName").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("lastName").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(employeeVertexType.getProperty("project"));
      assertEquals("project", employeeVertexType.getProperty("project").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("project").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(projectVertexType.getProperty("id"));
      assertEquals("id", projectVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("id").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(projectVertexType.getProperty("title"));
      assertEquals("title", projectVertexType.getProperty("title").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("title").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(projectVertexType.getProperty("projectManager"));
      assertEquals("projectManager", projectVertexType.getProperty("projectManager").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("projectManager").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      // edges check
      assertNotNull(worksAtProjectEdgeType);
      assertNotNull(hasManagerEdgeType);

      assertEquals("WorksAtProject", worksAtProjectEdgeType.getName());
      assertEquals("updatedOn", worksAtProjectEdgeType.getProperty("updatedOn").getName());
      assertEquals(OType.DATE, worksAtProjectEdgeType.getProperty("updatedOn").getType());
      assertEquals(true, worksAtProjectEdgeType.getProperty("empId").isMandatory());
      assertEquals(false, worksAtProjectEdgeType.getProperty("empId").isReadonly());
      assertEquals(false, worksAtProjectEdgeType.getProperty("empId").isNotNull());

      assertEquals("HasProjectManager", hasManagerEdgeType.getName());
      assertEquals("updatedOn", worksAtProjectEdgeType.getProperty("updatedOn").getName());
      assertEquals(OType.DATE, worksAtProjectEdgeType.getProperty("updatedOn").getType());
      assertEquals(false, worksAtProjectEdgeType.getProperty("empId").isMandatory());
      assertEquals(false, worksAtProjectEdgeType.getProperty("empId").isReadonly());
      assertEquals(false, worksAtProjectEdgeType.getProperty("empId").isNotNull());

      // Indices check
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Employee.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Employee", "empId"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Project.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Project", "id"));

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
      orientGraph.drop();
      orientGraph.shutdown();
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

      parentTableBuilding = "alter table EMPLOYEE add foreign key (PROJECT) references PROJECT(ID)";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      ODocument config = OFileManager.buildJsonFromFile("src/test/resources/configuration-mapping/relationships-mapping-inverted-edges.json");

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null, config);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfVertexType);
      assertEquals(2, context.getStatistics().wroteVertexType);
      assertEquals(2, context.getStatistics().totalNumberOfEdgeType);
      assertEquals(2, context.getStatistics().wroteEdgeType);
      assertEquals(2, context.getStatistics().totalNumberOfIndices);
      assertEquals(2, context.getStatistics().wroteIndexes);

      /*
       *  Testing built OrientDB schema
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      OrientVertexType employeeVertexType =  orientGraph.getVertexType("Employee");
      OrientVertexType projectVertexType = orientGraph.getVertexType("Project");
      OrientEdgeType hasEmployeeEdgeType = orientGraph.getEdgeType("HasEmployee");
      OrientEdgeType hasProjectManagerEdgeType = orientGraph.getEdgeType("HasProjectManager");

      // vertices check
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);

      // properties check
      assertNotNull(employeeVertexType.getProperty("empId"));
      assertEquals("empId", employeeVertexType.getProperty("empId").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("empId").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(employeeVertexType.getProperty("firstName"));
      assertEquals("firstName", employeeVertexType.getProperty("firstName").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("firstName").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(employeeVertexType.getProperty("lastName"));
      assertEquals("lastName", employeeVertexType.getProperty("lastName").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("lastName").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(employeeVertexType.getProperty("project"));
      assertEquals("project", employeeVertexType.getProperty("project").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("project").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(projectVertexType.getProperty("id"));
      assertEquals("id", projectVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("id").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(projectVertexType.getProperty("title"));
      assertEquals("title", projectVertexType.getProperty("title").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("title").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      assertNotNull(projectVertexType.getProperty("projectManager"));
      assertEquals("projectManager", projectVertexType.getProperty("projectManager").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("projectManager").getType());
      assertEquals(false, employeeVertexType.getProperty("empId").isMandatory());
      assertEquals(false, employeeVertexType.getProperty("empId").isReadonly());
      assertEquals(false, employeeVertexType.getProperty("empId").isNotNull());

      // edges check
      assertNotNull(hasEmployeeEdgeType);
      assertNotNull(hasProjectManagerEdgeType);

      assertEquals("HasEmployee", hasEmployeeEdgeType.getName());
      assertEquals("updatedOn", hasEmployeeEdgeType.getProperty("updatedOn").getName());
      assertEquals(OType.DATE, hasEmployeeEdgeType.getProperty("updatedOn").getType());
      assertEquals(true, hasEmployeeEdgeType.getProperty("empId").isMandatory());
      assertEquals(false, hasEmployeeEdgeType.getProperty("empId").isReadonly());
      assertEquals(false, hasEmployeeEdgeType.getProperty("empId").isNotNull());

      assertEquals("HasProjectManager", hasProjectManagerEdgeType.getName());
      assertEquals(0, hasProjectManagerEdgeType.propertiesMap().size());

      // Indices check
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Employee.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Employee", "empId"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Project.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Project", "id"));

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
      orientGraph.drop();
      orientGraph.shutdown();
    }

  }


}
