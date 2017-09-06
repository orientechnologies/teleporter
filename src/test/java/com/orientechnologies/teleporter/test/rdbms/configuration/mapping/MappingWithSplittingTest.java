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

package com.orientechnologies.teleporter.test.rdbms.configuration.mapping;

import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OEEClassMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OEVClassMapper;
import com.orientechnologies.teleporter.model.dbschema.OCanonicalRelationship;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OModelProperty;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.util.OFileManager;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class MappingWithSplittingTest {

  private OER2GraphMapper    mapper;
  private OTeleporterContext context;
  private final String config = "src/test/resources/configuration-mapping/splitting-into2tables-mapping.json";
  private ODBQueryEngine dbQueryEngine;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private OSourceDatabaseInfo sourceDBInfo;
  private String outParentDirectory = "embedded:target/";


  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance(this.outParentDirectory);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setMessageHandler(new OTeleporterMessageHandler(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.sourceDBInfo = new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
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
      this.mapper.buildSourceDatabaseSchema();
      this.mapper.buildGraphModel(new OJavaConventionNameResolver());
      this.mapper.applyImportConfiguration();


            /*
             *  Testing context information
             */

      assertEquals(3, context.getStatistics().totalNumberOfEntities);
      assertEquals(3, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);

      assertEquals(4, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(4, context.getStatistics().builtModelVertexTypes);
      assertEquals(3, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(3, context.getStatistics().builtModelEdgeTypes);


            /*
             *  Testing built source db schema
             */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE_PROJECT");
      OEntity departmentEntity = mapper.getDataBaseSchema().getEntityByName("DEPARTMENT");
      OEntity chiefOfficerEntity = mapper.getDataBaseSchema().getEntityByName("CHIEF_OFFICER");

      // entities check
      assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(departmentEntity);
      assertNotNull(chiefOfficerEntity);

      // attributes check
      assertEquals(7, employeeEntity.getAttributes().size());

      assertNotNull(employeeEntity.getAttributeByName("FIRST_NAME"));
      assertEquals("FIRST_NAME", employeeEntity.getAttributeByName("FIRST_NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("FIRST_NAME").getDataType());
      assertEquals(1, employeeEntity.getAttributeByName("FIRST_NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE_PROJECT", employeeEntity.getAttributeByName("FIRST_NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("LAST_NAME"));
      assertEquals("LAST_NAME", employeeEntity.getAttributeByName("LAST_NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("LAST_NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("LAST_NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE_PROJECT", employeeEntity.getAttributeByName("LAST_NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", employeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DOUBLE", employeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals("EMPLOYEE_PROJECT", employeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("DEPARTMENT"));
      assertEquals("DEPARTMENT", employeeEntity.getAttributeByName("DEPARTMENT").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("DEPARTMENT").getDataType());
      assertEquals(4, employeeEntity.getAttributeByName("DEPARTMENT").getOrdinalPosition());
      assertEquals("EMPLOYEE_PROJECT", employeeEntity.getAttributeByName("DEPARTMENT").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("PROJECT"));
      assertEquals("PROJECT", employeeEntity.getAttributeByName("PROJECT").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("PROJECT").getDataType());
      assertEquals(5, employeeEntity.getAttributeByName("PROJECT").getOrdinalPosition());
      assertEquals("EMPLOYEE_PROJECT", employeeEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("BALANCE"));
      assertEquals("BALANCE", employeeEntity.getAttributeByName("BALANCE").getName());
      assertEquals("DOUBLE", employeeEntity.getAttributeByName("BALANCE").getDataType());
      assertEquals(6, employeeEntity.getAttributeByName("BALANCE").getOrdinalPosition());
      assertEquals("EMPLOYEE_PROJECT", employeeEntity.getAttributeByName("BALANCE").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("ROLE"));
      assertEquals("ROLE", employeeEntity.getAttributeByName("ROLE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("ROLE").getDataType());
      assertEquals(7, employeeEntity.getAttributeByName("ROLE").getOrdinalPosition());
      assertEquals("EMPLOYEE_PROJECT", employeeEntity.getAttributeByName("ROLE").getBelongingEntity().getName());

      assertEquals(4, departmentEntity.getAttributes().size());

      assertNotNull(departmentEntity.getAttributeByName("ID"));
      assertEquals("ID", departmentEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", departmentEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, departmentEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("DEPARTMENT", departmentEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(departmentEntity.getAttributeByName("NAME"));
      assertEquals("NAME", departmentEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", departmentEntity.getAttributeByName("NAME").getDataType());
      assertEquals(2, departmentEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals("DEPARTMENT", departmentEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(departmentEntity.getAttributeByName("LOCATION"));
      assertEquals("LOCATION", departmentEntity.getAttributeByName("LOCATION").getName());
      assertEquals("VARCHAR", departmentEntity.getAttributeByName("LOCATION").getDataType());
      assertEquals(3, departmentEntity.getAttributeByName("LOCATION").getOrdinalPosition());
      assertEquals("DEPARTMENT", departmentEntity.getAttributeByName("LOCATION").getBelongingEntity().getName());

      assertNotNull(departmentEntity.getAttributeByName("UPDATED_ON"));
      assertEquals("UPDATED_ON", departmentEntity.getAttributeByName("UPDATED_ON").getName());
      assertEquals("DATE", departmentEntity.getAttributeByName("UPDATED_ON").getDataType());
      assertEquals(4, departmentEntity.getAttributeByName("UPDATED_ON").getOrdinalPosition());
      assertEquals("DEPARTMENT", departmentEntity.getAttributeByName("UPDATED_ON").getBelongingEntity().getName());

      assertEquals(3, chiefOfficerEntity.getAttributes().size());

      assertNotNull(chiefOfficerEntity.getAttributeByName("FIRST_NAME"));
      assertEquals("FIRST_NAME", chiefOfficerEntity.getAttributeByName("FIRST_NAME").getName());
      assertEquals("VARCHAR", chiefOfficerEntity.getAttributeByName("FIRST_NAME").getDataType());
      assertEquals(1, chiefOfficerEntity.getAttributeByName("FIRST_NAME").getOrdinalPosition());
      assertEquals("CHIEF_OFFICER", chiefOfficerEntity.getAttributeByName("FIRST_NAME").getBelongingEntity().getName());

      assertNotNull(chiefOfficerEntity.getAttributeByName("LAST_NAME"));
      assertEquals("LAST_NAME", chiefOfficerEntity.getAttributeByName("LAST_NAME").getName());
      assertEquals("VARCHAR", chiefOfficerEntity.getAttributeByName("LAST_NAME").getDataType());
      assertEquals(2, chiefOfficerEntity.getAttributeByName("LAST_NAME").getOrdinalPosition());
      assertEquals("CHIEF_OFFICER", chiefOfficerEntity.getAttributeByName("LAST_NAME").getBelongingEntity().getName());

      assertNotNull(chiefOfficerEntity.getAttributeByName("PROJECT"));
      assertEquals("PROJECT", chiefOfficerEntity.getAttributeByName("PROJECT").getName());
      assertEquals("VARCHAR", chiefOfficerEntity.getAttributeByName("PROJECT").getDataType());
      assertEquals(3, chiefOfficerEntity.getAttributeByName("PROJECT").getOrdinalPosition());
      assertEquals("CHIEF_OFFICER", chiefOfficerEntity.getAttributeByName("PROJECT").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
      assertEquals(0, departmentEntity.getOutCanonicalRelationships().size());
      assertEquals(1, chiefOfficerEntity.getOutCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getInCanonicalRelationships().size());
      assertEquals(1, departmentEntity.getInCanonicalRelationships().size());
      assertEquals(0, chiefOfficerEntity.getInCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(0, departmentEntity.getForeignKeys().size());
      assertEquals(1, chiefOfficerEntity.getForeignKeys().size());

      // Relationship: EMPLOYEE_PROJECT --> DEPARTMENT
      Iterator<OCanonicalRelationship> it = employeeEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("DEPARTMENT", currentRelationship.getParentEntity().getName());
      assertEquals("EMPLOYEE_PROJECT", currentRelationship.getForeignEntity().getName());
      assertEquals(departmentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = departmentEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("DEPARTMENT", employeeEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", departmentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());
      assertFalse(it2.hasNext());

      // Relationship: CHIEF_OFFICER --> EMPLOYEE_PROJECT (Project)
      it = chiefOfficerEntity.getOutCanonicalRelationships().iterator();
      currentRelationship = it.next();
      assertEquals("EMPLOYEE_PROJECT", currentRelationship.getParentEntity().getName());
      assertEquals("CHIEF_OFFICER", currentRelationship.getForeignEntity().getName());
      assertEquals(1, currentRelationship.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals("PROJECT", currentRelationship.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals(chiefOfficerEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      it2 = employeeEntity.getInCanonicalRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("DEPARTMENT", employeeEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", departmentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());
      assertFalse(it2.hasNext());


            /*
             *  Testing built graph model
             */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType projectVertexType = mapper.getGraphModel().getVertexTypeByName("Project");
      OVertexType departmentVertexType = mapper.getGraphModel().getVertexTypeByName("Department");
      OVertexType chiefOfficerVertexType = mapper.getGraphModel().getVertexTypeByName("ChiefOfficer");
      OEdgeType worksAtEdgeType = mapper.getGraphModel().getEdgeTypeByName("WorksAt");
      OEdgeType hasProjectEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasProject");
      OEdgeType isChiefForProjectEdgeType = mapper.getGraphModel().getEdgeTypeByName("IsChiefForProject");

      // vertices check
      assertEquals(4, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);
      assertNotNull(departmentVertexType);
      assertNotNull(chiefOfficerVertexType);

      // properties check
      assertEquals(4, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", employeeVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals("STRING", employeeVertexType.getPropertyByName("firstName").getOrientdbType());
      assertEquals(1, employeeVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("firstName").isFromPrimaryKey());
      assertEquals(true, employeeVertexType.getPropertyByName("firstName").isIncludedInMigration());

      assertNotNull(employeeVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", employeeVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals("STRING", employeeVertexType.getPropertyByName("lastName").getOrientdbType());
      assertEquals(2, employeeVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("lastName").isFromPrimaryKey());
      assertEquals(true, employeeVertexType.getPropertyByName("lastName").isIncludedInMigration());

      assertNotNull(employeeVertexType.getPropertyByName("salary"));
      assertEquals("salary", employeeVertexType.getPropertyByName("salary").getName());
      assertEquals("DOUBLE", employeeVertexType.getPropertyByName("salary").getOriginalType());
      assertEquals("DECIMAL", employeeVertexType.getPropertyByName("salary").getOrientdbType());
      assertEquals(3, employeeVertexType.getPropertyByName("salary").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("salary").isFromPrimaryKey());
      assertEquals(true, employeeVertexType.getPropertyByName("salary").isIncludedInMigration());

      assertNotNull(employeeVertexType.getPropertyByName("department"));
      assertEquals("department", employeeVertexType.getPropertyByName("department").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("department").getOriginalType());
      assertEquals("STRING", employeeVertexType.getPropertyByName("department").getOrientdbType());
      assertEquals(4, employeeVertexType.getPropertyByName("department").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("department").isFromPrimaryKey());
      assertEquals(true, employeeVertexType.getPropertyByName("department").isIncludedInMigration());

      assertEquals(2, employeeVertexType.getOutEdgesType().size());
      assertNotNull(employeeVertexType.getEdgeByName("WorksAt", ODirection.OUT));
      assertNotNull(employeeVertexType.getEdgeByName("HasProject", ODirection.OUT));
      assertEquals(0, employeeVertexType.getInEdgesType().size());

      assertEquals(2, projectVertexType.getProperties().size());

      assertNotNull(projectVertexType.getPropertyByName("project"));
      assertEquals("project", projectVertexType.getPropertyByName("project").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("project").getOriginalType());
      assertEquals("STRING", projectVertexType.getPropertyByName("project").getOrientdbType());
      assertEquals(1, projectVertexType.getPropertyByName("project").getOrdinalPosition());
      assertEquals(true, projectVertexType.getPropertyByName("project").isFromPrimaryKey());
      assertEquals(true, projectVertexType.getPropertyByName("project").isIncludedInMigration());

      assertNotNull(projectVertexType.getPropertyByName("balance"));
      assertEquals("balance", projectVertexType.getPropertyByName("balance").getName());
      assertEquals("DOUBLE", projectVertexType.getPropertyByName("balance").getOriginalType());
      assertEquals("DECIMAL", projectVertexType.getPropertyByName("balance").getOrientdbType());
      assertEquals(2, projectVertexType.getPropertyByName("balance").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("balance").isFromPrimaryKey());
      assertEquals(true, projectVertexType.getPropertyByName("balance").isIncludedInMigration());

      assertEquals(0, projectVertexType.getOutEdgesType().size());
      assertEquals(2, projectVertexType.getInEdgesType().size());
      assertEquals(hasProjectEdgeType, projectVertexType.getInEdgesType().get(0));
      assertEquals(isChiefForProjectEdgeType, projectVertexType.getInEdgesType().get(1));

      assertEquals(4, departmentVertexType.getProperties().size());

      assertNotNull(departmentVertexType.getPropertyByName("id"));
      assertEquals("id", departmentVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", departmentVertexType.getPropertyByName("id").getOriginalType());
      assertEquals("STRING", departmentVertexType.getPropertyByName("id").getOrientdbType());
      assertEquals(1, departmentVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, departmentVertexType.getPropertyByName("id").isFromPrimaryKey());
      assertEquals(true, departmentVertexType.getPropertyByName("id").isIncludedInMigration());

      assertNotNull(departmentVertexType.getPropertyByName("departmentName"));
      assertEquals("departmentName", departmentVertexType.getPropertyByName("departmentName").getName());
      assertEquals("VARCHAR", departmentVertexType.getPropertyByName("departmentName").getOriginalType());
      assertEquals("STRING", departmentVertexType.getPropertyByName("departmentName").getOrientdbType());
      assertEquals(2, departmentVertexType.getPropertyByName("departmentName").getOrdinalPosition());
      assertEquals(false, departmentVertexType.getPropertyByName("departmentName").isFromPrimaryKey());
      assertEquals(true, departmentVertexType.getPropertyByName("departmentName").isIncludedInMigration());

      assertNotNull(departmentVertexType.getPropertyByName("location"));
      assertEquals("location", departmentVertexType.getPropertyByName("location").getName());
      assertEquals("VARCHAR", departmentVertexType.getPropertyByName("location").getOriginalType());
      assertEquals("STRING", departmentVertexType.getPropertyByName("location").getOrientdbType());
      assertEquals(3, departmentVertexType.getPropertyByName("location").getOrdinalPosition());
      assertEquals(false, departmentVertexType.getPropertyByName("location").isFromPrimaryKey());
      assertEquals(true, departmentVertexType.getPropertyByName("location").isIncludedInMigration());

      assertNotNull(departmentVertexType.getPropertyByName("updatedOn"));
      assertEquals("updatedOn", departmentVertexType.getPropertyByName("updatedOn").getName());
      assertEquals("DATE", departmentVertexType.getPropertyByName("updatedOn").getOriginalType());
      assertEquals("DATE", departmentVertexType.getPropertyByName("updatedOn").getOrientdbType());
      assertEquals(4, departmentVertexType.getPropertyByName("updatedOn").getOrdinalPosition());
      assertEquals(false, departmentVertexType.getPropertyByName("updatedOn").isFromPrimaryKey());
      assertEquals(false, departmentVertexType.getPropertyByName("updatedOn").isIncludedInMigration());

      assertEquals(0, departmentVertexType.getOutEdgesType().size());
      assertEquals(1, departmentVertexType.getInEdgesType().size());
      assertEquals(worksAtEdgeType, departmentVertexType.getInEdgesType().get(0));

      assertEquals(3, chiefOfficerVertexType.getProperties().size());

      assertNotNull(chiefOfficerVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", chiefOfficerVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", chiefOfficerVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals("STRING", chiefOfficerVertexType.getPropertyByName("firstName").getOrientdbType());
      assertEquals(1, chiefOfficerVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(true, chiefOfficerVertexType.getPropertyByName("firstName").isFromPrimaryKey());
      assertEquals(true, chiefOfficerVertexType.getPropertyByName("firstName").isIncludedInMigration());

      assertNotNull(chiefOfficerVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", chiefOfficerVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", chiefOfficerVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals("STRING", chiefOfficerVertexType.getPropertyByName("lastName").getOrientdbType());
      assertEquals(2, chiefOfficerVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(true, chiefOfficerVertexType.getPropertyByName("lastName").isFromPrimaryKey());
      assertEquals(true, chiefOfficerVertexType.getPropertyByName("lastName").isIncludedInMigration());

      assertNotNull(chiefOfficerVertexType.getPropertyByName("project"));
      assertEquals("project", chiefOfficerVertexType.getPropertyByName("project").getName());
      assertEquals("VARCHAR", chiefOfficerVertexType.getPropertyByName("project").getOriginalType());
      assertEquals("STRING", chiefOfficerVertexType.getPropertyByName("project").getOrientdbType());
      assertEquals(3, chiefOfficerVertexType.getPropertyByName("project").getOrdinalPosition());
      assertEquals(false, chiefOfficerVertexType.getPropertyByName("project").isFromPrimaryKey());
      assertEquals(true, chiefOfficerVertexType.getPropertyByName("project").isIncludedInMigration());

      assertEquals(1, chiefOfficerVertexType.getOutEdgesType().size());
      assertEquals(0, chiefOfficerVertexType.getInEdgesType().size());
      assertEquals(isChiefForProjectEdgeType, chiefOfficerVertexType.getOutEdgesType().get(0));

      // edges check
      assertEquals(3, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(worksAtEdgeType);
      assertNotNull(hasProjectEdgeType);
      assertNotNull(isChiefForProjectEdgeType);

      assertEquals("WorksAt", worksAtEdgeType.getName());
      assertEquals(1, worksAtEdgeType.getProperties().size());
      assertEquals("Department", worksAtEdgeType.getInVertexType().getName());
      assertEquals(1, worksAtEdgeType.getNumberRelationshipsRepresented());
      assertFalse(worksAtEdgeType.isSplittingEdge());

      assertEquals(1, worksAtEdgeType.getAllProperties().size());
      OModelProperty sinceProperty = worksAtEdgeType.getPropertyByName("since");
      assertNotNull(sinceProperty);
      assertEquals("since", sinceProperty.getName());
      assertEquals(1, sinceProperty.getOrdinalPosition());
      assertEquals(false, sinceProperty.isFromPrimaryKey());
      assertEquals("DATE", sinceProperty.getOrientdbType());
      assertEquals(false, sinceProperty.isMandatory());
      assertEquals(false, sinceProperty.isReadOnly());
      assertEquals(false, sinceProperty.isNotNull());

      assertEquals("HasProject", hasProjectEdgeType.getName());
      assertEquals(1, hasProjectEdgeType.getProperties().size());
      assertEquals("Project", hasProjectEdgeType.getInVertexType().getName());
      assertEquals(0, hasProjectEdgeType.getNumberRelationshipsRepresented());
      assertTrue(hasProjectEdgeType.isSplittingEdge());

      assertEquals(1, hasProjectEdgeType.getAllProperties().size());
      OModelProperty roleProperty = hasProjectEdgeType.getPropertyByName("role");
      assertNotNull(roleProperty);
      assertEquals("role", roleProperty.getName());
      assertEquals(1, roleProperty.getOrdinalPosition());
      assertEquals(false, roleProperty.isFromPrimaryKey());
      assertEquals("VARCHAR", roleProperty.getOriginalType());
      assertEquals(false, roleProperty.isMandatory());
      assertEquals(false, roleProperty.isReadOnly());
      assertEquals(false, roleProperty.isNotNull());

      assertEquals("IsChiefForProject", isChiefForProjectEdgeType.getName());
      assertEquals(0, isChiefForProjectEdgeType.getProperties().size());
      assertEquals("Project", isChiefForProjectEdgeType.getInVertexType().getName());
      assertEquals(1, isChiefForProjectEdgeType.getNumberRelationshipsRepresented());
      assertFalse(isChiefForProjectEdgeType.isSplittingEdge());

      assertEquals(0, isChiefForProjectEdgeType.getAllProperties().size());


            /*
             * Rules check
             */

      // Classes Mapping

      assertEquals(4, mapper.getVertexType2EVClassMappers().size());
      assertEquals(3, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
      OEVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertEquals(2, mapper.getEVClassMappersByEntity(employeeEntity).size());
      assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));  //!!!
      assertEquals(employeeClassMapper.getEntity(), employeeEntity);
      assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

      assertEquals(4, employeeClassMapper.getAttribute2property().size());
      assertEquals(4, employeeClassMapper.getProperty2attribute().size());
      assertEquals("firstName", employeeClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", employeeClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("salary", employeeClassMapper.getAttribute2property().get("SALARY"));
      assertEquals("department", employeeClassMapper.getAttribute2property().get("DEPARTMENT"));
      assertEquals("FIRST_NAME", employeeClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", employeeClassMapper.getProperty2attribute().get("lastName"));
      assertEquals("SALARY", employeeClassMapper.getProperty2attribute().get("salary"));
      assertEquals("DEPARTMENT", employeeClassMapper.getProperty2attribute().get("department"));

      assertEquals(1, mapper.getEVClassMappersByVertex(projectVertexType).size());
      OEVClassMapper projectClassMapper = mapper.getEVClassMappersByVertex(projectVertexType).get(0);
      assertEquals(projectClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(1));
      assertEquals(projectClassMapper.getEntity(), employeeEntity);
      assertEquals(projectClassMapper.getVertexType(), projectVertexType);

      assertEquals(2, projectClassMapper.getAttribute2property().size());
      assertEquals(2, projectClassMapper.getProperty2attribute().size());
      assertEquals("project", projectClassMapper.getAttribute2property().get("PROJECT"));
      assertEquals("balance", projectClassMapper.getAttribute2property().get("BALANCE"));
      assertEquals("PROJECT", projectClassMapper.getProperty2attribute().get("project"));
      assertEquals("BALANCE", projectClassMapper.getProperty2attribute().get("balance"));

      assertEquals(1, mapper.getEVClassMappersByVertex(departmentVertexType).size());
      OEVClassMapper departmentClassMapper = mapper.getEVClassMappersByVertex(departmentVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(departmentEntity).size());
      assertEquals(departmentClassMapper, mapper.getEVClassMappersByEntity(departmentEntity).get(0));
      assertEquals(departmentClassMapper.getEntity(), departmentEntity);
      assertEquals(departmentClassMapper.getVertexType(), departmentVertexType);

      assertEquals(4, departmentClassMapper.getAttribute2property().size());
      assertEquals(4, departmentClassMapper.getProperty2attribute().size());
      assertEquals("id", departmentClassMapper.getAttribute2property().get("ID"));
      assertEquals("departmentName", departmentClassMapper.getAttribute2property().get("NAME"));
      assertEquals("location", departmentClassMapper.getAttribute2property().get("LOCATION"));
      assertEquals("updatedOn", departmentClassMapper.getAttribute2property().get("UPDATED_ON"));
      assertEquals("ID", departmentClassMapper.getProperty2attribute().get("id"));
      assertEquals("NAME", departmentClassMapper.getProperty2attribute().get("departmentName"));
      assertEquals("LOCATION", departmentClassMapper.getProperty2attribute().get("location"));
      assertEquals("UPDATED_ON", departmentClassMapper.getProperty2attribute().get("updatedOn"));

      assertEquals(1, mapper.getEdgeType2EEClassMappers().size());
      assertEquals(1, mapper.getEntity2EEClassMappers().size());

      assertEquals(1, mapper.getEEClassMappersByEdge(hasProjectEdgeType).size());
      OEEClassMapper hasProjectClassMapper = mapper.getEEClassMappersByEdge(hasProjectEdgeType).get(0);
      assertEquals(1, mapper.getEEClassMappersByEntity(employeeEntity).size());
      assertEquals(hasProjectClassMapper, mapper.getEEClassMappersByEntity(employeeEntity).get(0));  //!!!
      assertEquals(hasProjectClassMapper.getEntity(), employeeEntity);
      assertEquals(hasProjectClassMapper.getEdgeType(), hasProjectEdgeType);

      assertEquals(1, hasProjectClassMapper.getAttribute2property().size());
      assertEquals(1, hasProjectClassMapper.getProperty2attribute().size());
      assertEquals("role", hasProjectClassMapper.getAttribute2property().get("ROLE"));
      assertEquals("ROLE", hasProjectClassMapper.getProperty2attribute().get("role"));

      // Relationships-Edges Mapping

      Iterator<OCanonicalRelationship> itRelationships = employeeEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship worksAtRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(worksAtEdgeType, mapper.getRelationship2edgeType().get(worksAtRelationship));
      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(worksAtEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(worksAtEdgeType).contains(worksAtRelationship));

      itRelationships = chiefOfficerEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship isChiefForProjectRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(isChiefForProjectEdgeType, mapper.getRelationship2edgeType().get(isChiefForProjectRelationship));
      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(isChiefForProjectEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(isChiefForProjectEdgeType).contains(isChiefForProjectRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

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
