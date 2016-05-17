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

package com.orientdb.teleporter.test.rdbms.mapper;

import com.orientdb.teleporter.context.OOutputStreamManager;
import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientdb.teleporter.model.dbschema.OEntity;
import com.orientdb.teleporter.model.dbschema.ORelationship;
import com.orientdb.teleporter.model.graphmodel.OEdgeType;
import com.orientdb.teleporter.model.graphmodel.OModelProperty;
import com.orientdb.teleporter.model.graphmodel.OVertexType;
import com.orientdb.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientdb.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientdb.teleporter.util.OFileManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

public class OConfigurationMapping {

  private OER2GraphMapper    mapper;
  private  OTeleporterContext context;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    context.setOutputManager(new OOutputStreamManager(0));
    this.context.setQueryQuoteType("\"");
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


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().analizedRelationships);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);

       /*
       *  Testing built source db schema
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity projectEntity = mapper.getDataBaseSchema().getEntityByName("PROJECT");

      // entities check
      assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(projectEntity);

      // attributes check
      assertEquals(4, employeeEntity.getAttributes().size());

      assertNotNull(employeeEntity.getAttributeByName("EMP_ID"));
      assertEquals("EMP_ID", employeeEntity.getAttributeByName("EMP_ID").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("EMP_ID").getDataType());
      assertEquals(1, employeeEntity.getAttributeByName("EMP_ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("EMP_ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("FIRST_NAME"));
      assertEquals("FIRST_NAME", employeeEntity.getAttributeByName("FIRST_NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("FIRST_NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("FIRST_NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("FIRST_NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("LAST_NAME"));
      assertEquals("LAST_NAME", employeeEntity.getAttributeByName("LAST_NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("LAST_NAME").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("LAST_NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("LAST_NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("PROJECT"));
      assertEquals("PROJECT", employeeEntity.getAttributeByName("PROJECT").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("PROJECT").getDataType());
      assertEquals(4, employeeEntity.getAttributeByName("PROJECT").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

      assertEquals(3, projectEntity.getAttributes().size());

      assertNotNull(projectEntity.getAttributeByName("ID"));
      assertEquals("ID", projectEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", projectEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, projectEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("PROJECT", projectEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(projectEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", projectEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", projectEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(2, projectEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("PROJECT", projectEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(projectEntity.getAttributeByName("PROJECT_MANAGER"));
      assertEquals("PROJECT_MANAGER", projectEntity.getAttributeByName("PROJECT_MANAGER").getName());
      assertEquals("VARCHAR", projectEntity.getAttributeByName("PROJECT_MANAGER").getDataType());
      assertEquals(3, projectEntity.getAttributeByName("PROJECT_MANAGER").getOrdinalPosition());
      assertEquals("PROJECT", projectEntity.getAttributeByName("PROJECT_MANAGER").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(2, mapper.getDataBaseSchema().getRelationships().size());
      assertEquals(1, projectEntity.getOutRelationships().size());
      assertEquals(1, employeeEntity.getOutRelationships().size());
      assertEquals(1, projectEntity.getInRelationships().size());
      assertEquals(1, employeeEntity.getInRelationships().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(1, projectEntity.getForeignKeys().size());

      Iterator<ORelationship> it = projectEntity.getOutRelationships().iterator();
      ORelationship currentRelationship = it.next();
      assertEquals("EMPLOYEE", currentRelationship.getParentEntityName());
      assertEquals("PROJECT", currentRelationship.getForeignEntityName());
      assertEquals(employeeEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(projectEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<ORelationship> it2 = employeeEntity.getInRelationships().iterator();
      ORelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT_MANAGER", projectEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("EMP_ID", employeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

      it = employeeEntity.getOutRelationships().iterator();
      currentRelationship = it.next();
      assertEquals("PROJECT", currentRelationship.getParentEntityName());
      assertEquals("EMPLOYEE", currentRelationship.getForeignEntityName());
      assertEquals(projectEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      it2 = projectEntity.getInRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT", employeeEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", projectEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());


      /*
       *  Testing built graph model
       */
      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType projectVertexType = mapper.getGraphModel().getVertexByName("Project");
      OEdgeType worksAtEdgeType = mapper.getGraphModel().getEdgeTypeByName("WorksAtProject");
      OEdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");



      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);

      // properties check
      assertEquals(4, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("empId"));
      assertEquals("empId", employeeVertexType.getPropertyByName("empId").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("empId").getPropertyType());
      assertEquals(1, employeeVertexType.getPropertyByName("empId").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("empId").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", employeeVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("firstName").getPropertyType());
      assertEquals(2, employeeVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", employeeVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("lastName").getPropertyType());
      assertEquals(3, employeeVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("project"));
      assertEquals("project", employeeVertexType.getPropertyByName("project").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("project").getPropertyType());
      assertEquals(4, employeeVertexType.getPropertyByName("project").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("project").isFromPrimaryKey());

      assertEquals(1, employeeVertexType.getOutEdgesType().size());
      assertEquals(worksAtEdgeType, employeeVertexType.getOutEdgesType().get(0));
      assertEquals(1, employeeVertexType.getInEdgesType().size());
      assertEquals(hasManagerEdgeType, employeeVertexType.getInEdgesType().get(0));

      assertEquals(3, projectVertexType.getProperties().size());

      assertNotNull(projectVertexType.getPropertyByName("id"));
      assertEquals("id", projectVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, projectVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, projectVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("title"));
      assertEquals("title", projectVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("title").getPropertyType());
      assertEquals(2, projectVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("projectManager"));
      assertEquals("projectManager", projectVertexType.getPropertyByName("projectManager").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("projectManager").getPropertyType());
      assertEquals(3, projectVertexType.getPropertyByName("projectManager").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("projectManager").isFromPrimaryKey());

      assertEquals(1, projectVertexType.getOutEdgesType().size());
      assertEquals(hasManagerEdgeType, projectVertexType.getOutEdgesType().get(0));
      assertEquals(1, projectVertexType.getInEdgesType().size());
      assertEquals(worksAtEdgeType, projectVertexType.getInEdgesType().get(0));

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(worksAtEdgeType);
      assertNotNull(hasManagerEdgeType);

      assertEquals("WorksAtProject", worksAtEdgeType.getName());
      assertEquals(1, worksAtEdgeType.getProperties().size());
      assertEquals("Project", worksAtEdgeType.getInVertexType().getName());
      assertEquals(1, worksAtEdgeType.getNumberRelationshipsRepresented());

      assertEquals(1, worksAtEdgeType.getAllProperties().size());
      OModelProperty updatedOnProperty = worksAtEdgeType.getPropertyByName("updatedOn");
      assertNotNull(updatedOnProperty);
      assertEquals("updatedOn", updatedOnProperty.getName());
      assertEquals(1, updatedOnProperty.getOrdinalPosition());
      assertEquals(false, updatedOnProperty.isFromPrimaryKey());
      assertEquals("DATE", updatedOnProperty.getPropertyType());
      assertEquals(true, updatedOnProperty.isMandatory());
      assertEquals(false, updatedOnProperty.isReadOnly());
      assertEquals(false, updatedOnProperty.isNotNull());

      assertEquals("HasManager", hasManagerEdgeType.getName());
      assertEquals(1, hasManagerEdgeType.getProperties().size());
      assertEquals("Employee", hasManagerEdgeType.getInVertexType().getName());
      assertEquals(1, hasManagerEdgeType.getNumberRelationshipsRepresented());

      assertEquals(1, hasManagerEdgeType.getAllProperties().size());
      updatedOnProperty = hasManagerEdgeType.getPropertyByName("updatedOn");
      assertNotNull(updatedOnProperty);
      assertEquals("updatedOn", updatedOnProperty.getName());
      assertEquals(1, updatedOnProperty.getOrdinalPosition());
      assertEquals(false, updatedOnProperty.isFromPrimaryKey());
      assertEquals("DATE", updatedOnProperty.getPropertyType());
      assertEquals(false, updatedOnProperty.isMandatory());
      assertNull(updatedOnProperty.isReadOnly());
      assertNull(updatedOnProperty.isNotNull());


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

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String parentTableBuilding = "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"+
          " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, PROJECT varchar(256) not null, primary key (EMP_ID))"; //foreign key (PROJECT) references PROJECT(ID)
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

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().analizedRelationships);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity projectEntity = mapper.getDataBaseSchema().getEntityByName("PROJECT");

      // entities check
      assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(projectEntity);

      // attributes check
      assertEquals(4, employeeEntity.getAttributes().size());

      assertNotNull(employeeEntity.getAttributeByName("EMP_ID"));
      assertEquals("EMP_ID", employeeEntity.getAttributeByName("EMP_ID").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("EMP_ID").getDataType());
      assertEquals(1, employeeEntity.getAttributeByName("EMP_ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("EMP_ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("FIRST_NAME"));
      assertEquals("FIRST_NAME", employeeEntity.getAttributeByName("FIRST_NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("FIRST_NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("FIRST_NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("FIRST_NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("LAST_NAME"));
      assertEquals("LAST_NAME", employeeEntity.getAttributeByName("LAST_NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("LAST_NAME").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("LAST_NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("LAST_NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("PROJECT"));
      assertEquals("PROJECT", employeeEntity.getAttributeByName("PROJECT").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("PROJECT").getDataType());
      assertEquals(4, employeeEntity.getAttributeByName("PROJECT").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

      assertEquals(3, projectEntity.getAttributes().size());

      assertNotNull(projectEntity.getAttributeByName("ID"));
      assertEquals("ID", projectEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", projectEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, projectEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("PROJECT", projectEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(projectEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", projectEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", projectEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(2, projectEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("PROJECT", projectEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(projectEntity.getAttributeByName("PROJECT_MANAGER"));
      assertEquals("PROJECT_MANAGER", projectEntity.getAttributeByName("PROJECT_MANAGER").getName());
      assertEquals("VARCHAR", projectEntity.getAttributeByName("PROJECT_MANAGER").getDataType());
      assertEquals(3, projectEntity.getAttributeByName("PROJECT_MANAGER").getOrdinalPosition());
      assertEquals("PROJECT", projectEntity.getAttributeByName("PROJECT_MANAGER").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(2, mapper.getDataBaseSchema().getRelationships().size());
      assertEquals(1, projectEntity.getOutRelationships().size());
      assertEquals(1, employeeEntity.getOutRelationships().size());
      assertEquals(1, projectEntity.getInRelationships().size());
      assertEquals(1, employeeEntity.getInRelationships().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(1, projectEntity.getForeignKeys().size());

      Iterator<ORelationship> it = projectEntity.getOutRelationships().iterator();
      ORelationship currentRelationship = it.next();
      assertEquals("EMPLOYEE", currentRelationship.getParentEntityName());
      assertEquals("PROJECT", currentRelationship.getForeignEntityName());
      assertEquals(employeeEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(projectEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<ORelationship> it2 = employeeEntity.getInRelationships().iterator();
      ORelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT_MANAGER", projectEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("EMP_ID", employeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

      it = employeeEntity.getOutRelationships().iterator();
      currentRelationship = it.next();
      assertEquals("PROJECT", currentRelationship.getParentEntityName());
      assertEquals("EMPLOYEE", currentRelationship.getForeignEntityName());
      assertEquals(projectEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      it2 = projectEntity.getInRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT", employeeEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", projectEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());


      /*
       *  Testing built graph model
       */
      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType projectVertexType = mapper.getGraphModel().getVertexByName("Project");
      OEdgeType hasEmployeeEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasEmployee");
      OEdgeType hasProjectManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasProjectManager");



      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);

      // properties check
      assertEquals(4, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("empId"));
      assertEquals("empId", employeeVertexType.getPropertyByName("empId").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("empId").getPropertyType());
      assertEquals(1, employeeVertexType.getPropertyByName("empId").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("empId").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", employeeVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("firstName").getPropertyType());
      assertEquals(2, employeeVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", employeeVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("lastName").getPropertyType());
      assertEquals(3, employeeVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("project"));
      assertEquals("project", employeeVertexType.getPropertyByName("project").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("project").getPropertyType());
      assertEquals(4, employeeVertexType.getPropertyByName("project").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("project").isFromPrimaryKey());

      assertEquals(0, employeeVertexType.getOutEdgesType().size());
      assertEquals(2, employeeVertexType.getInEdgesType().size());
      assertEquals(hasEmployeeEdgeType, employeeVertexType.getInEdgesType().get(0));
      assertEquals(hasProjectManagerEdgeType, employeeVertexType.getInEdgesType().get(1));

      assertEquals(3, projectVertexType.getProperties().size());

      assertNotNull(projectVertexType.getPropertyByName("id"));
      assertEquals("id", projectVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, projectVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, projectVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("title"));
      assertEquals("title", projectVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("title").getPropertyType());
      assertEquals(2, projectVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("projectManager"));
      assertEquals("projectManager", projectVertexType.getPropertyByName("projectManager").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("projectManager").getPropertyType());
      assertEquals(3, projectVertexType.getPropertyByName("projectManager").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("projectManager").isFromPrimaryKey());

      assertEquals(2, projectVertexType.getOutEdgesType().size());
      assertEquals(hasEmployeeEdgeType, projectVertexType.getOutEdgesType().get(0));
      assertEquals(hasProjectManagerEdgeType, projectVertexType.getOutEdgesType().get(1));
      assertEquals(0, projectVertexType.getInEdgesType().size());

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(hasEmployeeEdgeType);
      assertNotNull(hasProjectManagerEdgeType);

      assertEquals("HasEmployee", hasEmployeeEdgeType.getName());
      assertEquals(1, hasEmployeeEdgeType.getProperties().size());
      assertEquals("Employee", hasEmployeeEdgeType.getInVertexType().getName());
      assertEquals(1, hasEmployeeEdgeType.getNumberRelationshipsRepresented());

      assertEquals(1, hasEmployeeEdgeType.getAllProperties().size());
      OModelProperty updatedOnProperty = hasEmployeeEdgeType.getPropertyByName("updatedOn");
      assertNotNull(updatedOnProperty);
      assertEquals("updatedOn", updatedOnProperty.getName());
      assertEquals(1, updatedOnProperty.getOrdinalPosition());
      assertEquals(false, updatedOnProperty.isFromPrimaryKey());
      assertEquals("DATE", updatedOnProperty.getPropertyType());
      assertEquals(true, updatedOnProperty.isMandatory());
      assertEquals(false, updatedOnProperty.isReadOnly());
      assertEquals(false, updatedOnProperty.isNotNull());

      assertEquals("HasProjectManager", hasProjectManagerEdgeType.getName());
      assertEquals(0, hasProjectManagerEdgeType.getProperties().size());
      assertEquals("Employee", hasProjectManagerEdgeType.getInVertexType().getName());
      assertEquals(1, hasProjectManagerEdgeType.getNumberRelationshipsRepresented());

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
    }
  }

}
