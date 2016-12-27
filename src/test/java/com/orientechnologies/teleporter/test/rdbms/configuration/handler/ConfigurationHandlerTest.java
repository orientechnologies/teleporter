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

package com.orientechnologies.teleporter.test.rdbms.configuration.handler;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.*;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.util.ODocumentComparator;
import com.orientechnologies.teleporter.util.OFileManager;

/**
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */

public class ConfigurationHandlerTest {

  private final String config1 = "src/test/resources/configuration-mapping/aggregation-from2tables-mapping.json";
  private final String config2 = "src/test/resources/configuration-mapping/joint-table-relationships-mapping-direct-edges.json";
  private final String config3 = "src/test/resources/configuration-mapping/config-handler-output.json";
  private final String config4 = "src/test/resources/configuration-mapping/splitting-into2tables-mapping.json";
  private OTeleporterContext   context;
  private ODBMSDataTypeHandler dataTypeHandler;

  private OER2GraphMapper mapper;
  private ODBQueryEngine  dbQueryEngine;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private OSourceDatabaseInfo sourceDBInfo;

  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setExecutionStrategy("naive-aggregate");
    this.dataTypeHandler = new OHSQLDBDataTypeHandler();
    this.context.setDataTypeHandler(dataTypeHandler);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.sourceDBInfo = new OSourceDatabaseInfo("hsqldb", this.driver, this.jurl, this.username, this.password);
  }

  @Test
  /**
   * Testing OConfiguration building from JSON (case 1)
   */

  public void test1() {

    OConfigurationHandler configurationHandler = new OConfigurationHandler(false);

    ODocument inputConfigurationDoc = null;
    try {
      inputConfigurationDoc = OFileManager.buildJsonFromFile(this.config1);
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }

    OConfiguration configuration = configurationHandler.buildConfigurationFromJSONDoc(inputConfigurationDoc);

    /**
     * Checking configured vertices
     */

    assertEquals(2, configuration.getConfiguredVertices().size());

    // person vertex class

    OConfiguredVertexClass personVertexClass = configuration.getConfiguredVertices().get(0);
    OVertexMappingInformation personMapping = personVertexClass.getMapping();

    assertEquals("Person", personVertexClass.getName());
    assertNotNull(personMapping);
    assertEquals(personVertexClass, personMapping.getBelongingVertex());
    assertEquals("equality", personMapping.getAggregationFunction());
    assertNotNull(personMapping.getSourceTables());
    assertEquals(2, personMapping.getSourceTables().size());

    OSourceTable personSourceTable = personMapping.getSourceTables().get(0);
    OSourceTable vatProfileSourceTable = personMapping.getSourceTables().get(1);

    assertNotNull(personSourceTable);
    assertNotNull(vatProfileSourceTable);
    assertEquals("sourceTable1", personSourceTable.getSourceIdName());
    assertEquals("hsqldb", personSourceTable.getDataSource());
    assertEquals("PERSON", personSourceTable.getTableName());
    assertNotNull(personSourceTable.getAggregationColumns());
    assertEquals(1, personSourceTable.getAggregationColumns().size());
    assertEquals("ID", personSourceTable.getAggregationColumns().get(0));
    assertEquals("sourceTable2", vatProfileSourceTable.getSourceIdName());
    assertEquals("hsqldb", vatProfileSourceTable.getDataSource());
    assertEquals("VAT_PROFILE", vatProfileSourceTable.getTableName());
    assertNotNull(vatProfileSourceTable.getAggregationColumns());
    assertEquals(1, vatProfileSourceTable.getAggregationColumns().size());
    assertEquals("ID", vatProfileSourceTable.getAggregationColumns().get(0));

    List<OConfiguredProperty> properties = new ArrayList<>(personVertexClass.getConfiguredProperties());
    assertNotNull(properties);
    assertEquals(7, properties.size());

    OConfiguredProperty currentConfiguredProperty = properties.get(0);
    OConfiguredPropertyMapping currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("extKey1", currentConfiguredProperty.getPropertyName());
    assertEquals("STRING", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(false, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(false, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
    assertEquals("ID", currentPropertyMapping.getColumnName());
    assertEquals("VARCHAR", currentPropertyMapping.getType());

    currentConfiguredProperty = properties.get(1);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("firstName", currentConfiguredProperty.getPropertyName());
    assertEquals("STRING", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(true, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(true, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
    assertEquals("NAME", currentPropertyMapping.getColumnName());
    assertEquals("VARCHAR", currentPropertyMapping.getType());

    currentConfiguredProperty = properties.get(2);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("lastName", currentConfiguredProperty.getPropertyName());
    assertEquals("STRING", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(true, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(true, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
    assertEquals("SURNAME", currentPropertyMapping.getColumnName());
    assertEquals("VARCHAR", currentPropertyMapping.getType());

    currentConfiguredProperty = properties.get(3);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("depId", currentConfiguredProperty.getPropertyName());
    assertEquals("STRING", currentConfiguredProperty.getPropertyType());
    assertEquals(false, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(false, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(false, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
    assertEquals("DEP_ID", currentPropertyMapping.getColumnName());
    assertEquals("VARCHAR", currentPropertyMapping.getType());

    currentConfiguredProperty = properties.get(4);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("extKey2", currentConfiguredProperty.getPropertyName());
    assertEquals("STRING", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(false, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(false, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable2", currentPropertyMapping.getSourceName());
    assertEquals("ID", currentPropertyMapping.getColumnName());
    assertEquals("VARCHAR", currentPropertyMapping.getType());

    currentConfiguredProperty = properties.get(5);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("VAT", currentConfiguredProperty.getPropertyName());
    assertEquals("STRING", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(true, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(true, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable2", currentPropertyMapping.getSourceName());
    assertEquals("VAT", currentPropertyMapping.getColumnName());
    assertEquals("VARCHAR", currentPropertyMapping.getType());

    currentConfiguredProperty = properties.get(6);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("updatedOn", currentConfiguredProperty.getPropertyName());
    assertEquals("DATE", currentConfiguredProperty.getPropertyType());
    assertEquals(false, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(true, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(true, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable2", currentPropertyMapping.getSourceName());
    assertEquals("UPDATED_ON", currentPropertyMapping.getColumnName());
    assertEquals("DATE", currentPropertyMapping.getType());

    // department vertex class

    OConfiguredVertexClass departmentVertexClass = configuration.getConfiguredVertices().get(1);
    OVertexMappingInformation departmentMapping = departmentVertexClass.getMapping();

    assertEquals("Department", departmentVertexClass.getName());
    assertNotNull(departmentMapping);
    assertEquals(departmentVertexClass, departmentMapping.getBelongingVertex());
    assertNull(departmentMapping.getAggregationFunction());
    assertNotNull(departmentMapping.getSourceTables());
    assertEquals(1, departmentMapping.getSourceTables().size());

    OSourceTable departmentSourceTable = departmentMapping.getSourceTables().get(0);

    assertNotNull(departmentSourceTable);
    assertEquals("sourceTable1", departmentSourceTable.getSourceIdName());
    assertEquals("mysql", departmentSourceTable.getDataSource());
    assertEquals("DEPARTMENT", departmentSourceTable.getTableName());
    assertNull(departmentSourceTable.getAggregationColumns());

    properties = new ArrayList<OConfiguredProperty>(departmentVertexClass.getConfiguredProperties());
    assertNotNull(properties);
    assertEquals(4, properties.size());

    currentConfiguredProperty = properties.get(0);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("id", currentConfiguredProperty.getPropertyName());
    assertEquals("STRING", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(false, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(false, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
    assertEquals("ID", currentPropertyMapping.getColumnName());
    assertEquals("VARCHAR", currentPropertyMapping.getType());

    currentConfiguredProperty = properties.get(1);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("departmentName", currentConfiguredProperty.getPropertyName());
    assertEquals("STRING", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(true, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(true, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
    assertEquals("NAME", currentPropertyMapping.getColumnName());
    assertEquals("VARCHAR", currentPropertyMapping.getType());

    currentConfiguredProperty = properties.get(2);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("location", currentConfiguredProperty.getPropertyName());
    assertEquals("STRING", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(true, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(true, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
    assertEquals("LOCATION", currentPropertyMapping.getColumnName());
    assertEquals("VARCHAR", currentPropertyMapping.getType());

    currentConfiguredProperty = properties.get(3);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNotNull(currentPropertyMapping);
    assertEquals("updatedOn", currentConfiguredProperty.getPropertyName());
    assertEquals("DATE", currentConfiguredProperty.getPropertyType());
    assertEquals(false, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(true, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(true, currentConfiguredProperty.isNotNull());
    assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
    assertEquals("UPDATED_ON", currentPropertyMapping.getColumnName());
    assertEquals("DATE", currentPropertyMapping.getType());

    /**
     * Checking configured edges
     */

    assertEquals(1, configuration.getConfiguredEdges().size());

    OConfiguredEdgeClass worksAtEdgeClass = configuration.getConfiguredEdges().get(0);
    assertEquals("WorksAt", worksAtEdgeClass.getName());

    List<OEdgeMappingInformation> worksAtMappings = worksAtEdgeClass.getMappings();
    assertEquals(1, worksAtMappings.size());
    OEdgeMappingInformation worksAtMapping1 = worksAtMappings.get(0);
    assertNotNull(worksAtMapping1);
    assertEquals(worksAtEdgeClass, worksAtMapping1.getBelongingEdge());
    assertEquals("PERSON", worksAtMapping1.getFromTableName());
    assertEquals(1, worksAtMapping1.getFromColumns().size());
    assertEquals("DEP_ID", worksAtMapping1.getFromColumns().get(0));
    assertEquals("DEPARTMENT", worksAtMapping1.getToTableName());
    assertEquals(1, worksAtMapping1.getToColumns().size());
    assertEquals("ID", worksAtMapping1.getToColumns().get(0));
    assertEquals("direct", worksAtMapping1.getDirection());
    assertNull(worksAtMapping1.getRepresentedJoinTableMapping());

    properties = new ArrayList<OConfiguredProperty>(worksAtEdgeClass.getConfiguredProperties());
    assertNotNull(properties);
    assertEquals(1, properties.size());

    currentConfiguredProperty = properties.get(0);
    currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNull(currentPropertyMapping);
    assertEquals("since", currentConfiguredProperty.getPropertyName());
    assertEquals("DATE", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(true, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(false, currentConfiguredProperty.isNotNull());

    /**
     * 1. Writing the configuration on a second JSON document through the configurationHandler. 2. Checking that the original JSON
     * configuration and the final just written configuration are equal.
     */
    ODocument writtenJsonConfiguration = configurationHandler.buildJSONDocFromConfiguration(configuration);
    assertTrue(ODocumentComparator.areEquals(inputConfigurationDoc, writtenJsonConfiguration));
  }

  @Test
  /**
   * Testing OConfiguration building from JSON (case 2)
   */

  public void test2() {

    OConfigurationHandler configurationHandler = new OConfigurationHandler(true);

    ODocument inputConfigurationDoc = null;
    try {
      inputConfigurationDoc = OFileManager.buildJsonFromFile(this.config2);
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }

    OConfiguration configuration = configurationHandler.buildConfigurationFromJSONDoc(inputConfigurationDoc);

    /**
     * Checking configured vertices
     */

    assertEquals(0, configuration.getConfiguredVertices().size());

    /**
     * Checking configured edges
     */

    assertEquals(1, configuration.getConfiguredEdges().size());

    OConfiguredEdgeClass performsEdgeClass = configuration.getConfiguredEdges().get(0);
    assertEquals("Performs", performsEdgeClass.getName());

    List<OEdgeMappingInformation> performsMappings = performsEdgeClass.getMappings();
    assertEquals(1, performsMappings.size());
    OEdgeMappingInformation performsMapping1 = performsMappings.get(0);
    assertNotNull(performsMapping1);
    assertEquals(performsEdgeClass, performsMapping1.getBelongingEdge());
    assertEquals("ACTOR", performsMapping1.getFromTableName());
    assertEquals(1, performsMapping1.getFromColumns().size());
    assertEquals("ID", performsMapping1.getFromColumns().get(0));
    assertEquals("FILM", performsMapping1.getToTableName());
    assertEquals(1, performsMapping1.getToColumns().size());
    assertEquals("ID", performsMapping1.getToColumns().get(0));
    assertEquals("direct", performsMapping1.getDirection());
    assertNotNull(performsMapping1.getRepresentedJoinTableMapping());
    assertEquals("ACTOR_FILM", performsMapping1.getRepresentedJoinTableMapping().getTableName());
    assertEquals(1, performsMapping1.getRepresentedJoinTableMapping().getFromColumns().size());
    assertEquals("ACTOR_ID", performsMapping1.getRepresentedJoinTableMapping().getFromColumns().get(0));
    assertEquals(1, performsMapping1.getRepresentedJoinTableMapping().getToColumns().size());
    assertEquals("FILM_ID", performsMapping1.getRepresentedJoinTableMapping().getToColumns().get(0));

    List<OConfiguredProperty> properties = new ArrayList<OConfiguredProperty>(performsEdgeClass.getConfiguredProperties());
    assertNotNull(properties);
    assertEquals(1, properties.size());

    OConfiguredProperty currentConfiguredProperty = properties.get(0);
    OConfiguredPropertyMapping currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
    assertNotNull(currentConfiguredProperty);
    assertNull(currentPropertyMapping);
    assertEquals("year", currentConfiguredProperty.getPropertyName());
    assertEquals("DATE", currentConfiguredProperty.getPropertyType());
    assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
    assertEquals(true, currentConfiguredProperty.isMandatory());
    assertEquals(false, currentConfiguredProperty.isReadOnly());
    assertEquals(false, currentConfiguredProperty.isNotNull());

    /**
     * 1. Writing the configuration on a second JSON document through the configurationHandler. 2. Checking that the original JSON
     * configuration and the final just written configuration are equal.
     */
    ODocument writtenJsonConfiguration = configurationHandler.buildJSONDocFromConfiguration(configuration);
    assertTrue(ODocumentComparator.areEquals(inputConfigurationDoc, writtenJsonConfiguration));

  }

  @Test
  /**
   * Testing: - OConfiguration building from Graph Model (case 1)
   */

  public void test3() {

    OConfigurationHandler configurationHandler = new OConfigurationHandler(true);
    Connection connection = null;
    Statement st = null;

    try {

      /**
       * Graph model building
       */

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String departmentTableBuilding =
          "create memory table DEPARTMENT (ID varchar(256) not null, DEPARTMENT_NAME  varchar(256) not null,"
              + " LOCATION varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(departmentTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, SALARY double not null,"
          + " EMAIL varchar(256) not null, DEPARTMENT varchar(256) not null, primary key (ID),"
          + " foreign key (DEPARTMENT) references DEPARTMENT(ID))";
      st.execute(employeeTableBuilding);

      String projectTableBuilding = "create memory table PROJECT (ID varchar(256) not null, PROJECT_NAME  varchar(256),"
          + " DESCRIPTION varchar(256) not null, START_DATE date not null, EXPECTED_END_DATE date not null, primary key (ID))";
      st.execute(projectTableBuilding);

      String projectEmployeeTableBuilding =
          "create memory table EMPLOYEE_PROJECT (EMPLOYEE_ID  varchar(256)not null, PROJECT_ID varchar(256) not null,"
              + " ROLE varchar(256) not null, primary key (EMPLOYEE_ID, PROJECT_ID), foreign key (EMPLOYEE_ID) references EMPLOYEE(ID),"
              + " foreign key (PROJECT_ID) references PROJECT(ID))";
      st.execute(projectEmployeeTableBuilding);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null);
      this.mapper.buildSourceDatabaseSchema();
      this.mapper.buildGraphModel(new OJavaConventionNameResolver());
      this.mapper.performMany2ManyAggregation();

      /**
       * Testing OConfiguration building
       */

      OConfiguration configuredGraph = configurationHandler.buildConfigurationFromMapper(this.mapper);

      assertEquals(3, configuredGraph.getConfiguredVertices().size());
      assertEquals(2, configuredGraph.getConfiguredEdges().size());

      OConfiguredVertexClass employeeConfiguredVertex = configuredGraph.getVertexClassByName("Employee");
      OConfiguredVertexClass projectConfiguredVertex = configuredGraph.getVertexClassByName("Project");
      OConfiguredVertexClass departmentConfiguredVertex = configuredGraph.getVertexClassByName("Department");
      OConfiguredEdgeClass employee2projectConfiguredEdge = configuredGraph.getEdgeClassByName("EmployeeProject");
      OConfiguredEdgeClass hasDepartmentConfiguredEdge = configuredGraph.getEdgeClassByName("HasDepartment");

      /**
       * Employee vertex check
       */

      assertEquals("Employee", employeeConfiguredVertex.getName());

      // mapping and source tables
      OVertexMappingInformation vertexMapping = employeeConfiguredVertex.getMapping();
      assertEquals(employeeConfiguredVertex, vertexMapping.getBelongingVertex());
      assertNull(vertexMapping.getAggregationFunction());
      List<OSourceTable> sourceTables = vertexMapping.getSourceTables();
      assertEquals(1, sourceTables.size());
      OSourceTable sourceTable = sourceTables.get(0);
      assertNull(sourceTable.getAggregationColumns());
      assertEquals("hsqldb_EMPLOYEE", sourceTable.getSourceIdName());
      assertEquals("hsqldb", sourceTable.getDataSource());
      assertEquals("EMPLOYEE", sourceTable.getTableName());

      // properties check
      List<OConfiguredProperty> configuredProperties = new ArrayList<OConfiguredProperty>(
          employeeConfiguredVertex.getConfiguredProperties());
      assertEquals(6, configuredProperties.size());

      OConfiguredProperty currConfiguredProperty = configuredProperties.get(0);
      assertEquals("id", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      OConfiguredPropertyMapping currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_EMPLOYEE", currPropertyMapping.getSourceName());
      assertEquals("ID", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(1);
      assertEquals("firstName", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_EMPLOYEE", currPropertyMapping.getSourceName());
      assertEquals("FIRST_NAME", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(2);
      assertEquals("lastName", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_EMPLOYEE", currPropertyMapping.getSourceName());
      assertEquals("LAST_NAME", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(3);
      assertEquals("salary", currConfiguredProperty.getPropertyName());
      assertEquals("DOUBLE", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_EMPLOYEE", currPropertyMapping.getSourceName());
      assertEquals("SALARY", currPropertyMapping.getColumnName());
      assertEquals("DOUBLE", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(4);
      assertEquals("email", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_EMPLOYEE", currPropertyMapping.getSourceName());
      assertEquals("EMAIL", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(5);
      assertEquals("department", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_EMPLOYEE", currPropertyMapping.getSourceName());
      assertEquals("DEPARTMENT", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      /**
       * Project vertex check
       */

      assertEquals("Project", projectConfiguredVertex.getName());

      // mapping and source tables
      vertexMapping = projectConfiguredVertex.getMapping();
      assertEquals(projectConfiguredVertex, vertexMapping.getBelongingVertex());
      assertNull(vertexMapping.getAggregationFunction());
      sourceTables = vertexMapping.getSourceTables();
      assertEquals(1, sourceTables.size());
      sourceTable = sourceTables.get(0);
      assertNull(sourceTable.getAggregationColumns());
      assertEquals("hsqldb_PROJECT", sourceTable.getSourceIdName());
      assertEquals("hsqldb", sourceTable.getDataSource());
      assertEquals("PROJECT", sourceTable.getTableName());

      // properties check
      configuredProperties = new ArrayList<OConfiguredProperty>(projectConfiguredVertex.getConfiguredProperties());
      assertEquals(5, configuredProperties.size());

      currConfiguredProperty = configuredProperties.get(0);
      assertEquals("id", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_PROJECT", currPropertyMapping.getSourceName());
      assertEquals("ID", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(1);
      assertEquals("projectName", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_PROJECT", currPropertyMapping.getSourceName());
      assertEquals("PROJECT_NAME", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(2);
      assertEquals("description", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_PROJECT", currPropertyMapping.getSourceName());
      assertEquals("DESCRIPTION", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(3);
      assertEquals("startDate", currConfiguredProperty.getPropertyName());
      assertEquals("DATE", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_PROJECT", currPropertyMapping.getSourceName());
      assertEquals("START_DATE", currPropertyMapping.getColumnName());
      assertEquals("DATE", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(4);
      assertEquals("expectedEndDate", currConfiguredProperty.getPropertyName());
      assertEquals("DATE", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_PROJECT", currPropertyMapping.getSourceName());
      assertEquals("EXPECTED_END_DATE", currPropertyMapping.getColumnName());
      assertEquals("DATE", currPropertyMapping.getType());

      /**
       * Department vertex check
       */

      assertEquals("Department", departmentConfiguredVertex.getName());

      // mapping and source tables
      vertexMapping = departmentConfiguredVertex.getMapping();
      assertEquals(departmentConfiguredVertex, vertexMapping.getBelongingVertex());
      assertNull(vertexMapping.getAggregationFunction());
      sourceTables = vertexMapping.getSourceTables();
      assertEquals(1, sourceTables.size());
      sourceTable = sourceTables.get(0);
      assertNull(sourceTable.getAggregationColumns());
      assertEquals("hsqldb_DEPARTMENT", sourceTable.getSourceIdName());
      assertEquals("hsqldb", sourceTable.getDataSource());
      assertEquals("DEPARTMENT", sourceTable.getTableName());

      // properties check
      configuredProperties = new ArrayList<OConfiguredProperty>(departmentConfiguredVertex.getConfiguredProperties());
      assertEquals(3, configuredProperties.size());

      currConfiguredProperty = configuredProperties.get(0);
      assertEquals("id", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_DEPARTMENT", currPropertyMapping.getSourceName());
      assertEquals("ID", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(1);
      assertEquals("departmentName", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_DEPARTMENT", currPropertyMapping.getSourceName());
      assertEquals("DEPARTMENT_NAME", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      currConfiguredProperty = configuredProperties.get(2);
      assertEquals("location", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertEquals("hsqldb_DEPARTMENT", currPropertyMapping.getSourceName());
      assertEquals("LOCATION", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      /**
       * Project2Employee edge check
       */

      assertEquals("EmployeeProject", employee2projectConfiguredEdge.getName());

      // mapping check
      List<OEdgeMappingInformation> mappings = employee2projectConfiguredEdge.getMappings();
      assertEquals(1, mappings.size());
      OEdgeMappingInformation edgeMapping = mappings.get(0);
      assertEquals(employee2projectConfiguredEdge, edgeMapping.getBelongingEdge());
      assertEquals("EMPLOYEE", edgeMapping.getFromTableName());
      assertEquals("PROJECT", edgeMapping.getToTableName());
      assertEquals(1, edgeMapping.getFromColumns().size());
      assertEquals("ID", edgeMapping.getFromColumns().get(0));
      assertEquals(1, edgeMapping.getToColumns().size());
      assertEquals("ID", edgeMapping.getToColumns().get(0));
      assertEquals("direct", edgeMapping.getDirection());
      OAggregatedJoinTableMapping joinTableMapping = edgeMapping.getRepresentedJoinTableMapping();
      assertNotNull(joinTableMapping);
      assertEquals("EMPLOYEE_PROJECT", joinTableMapping.getTableName());
      assertEquals(1, joinTableMapping.getFromColumns().size());
      assertEquals("EMPLOYEE_ID", joinTableMapping.getFromColumns().get(0));
      assertEquals(1, joinTableMapping.getToColumns().size());
      assertEquals("PROJECT_ID", joinTableMapping.getToColumns().get(0));

      // properties check
      configuredProperties = new ArrayList<OConfiguredProperty>(employee2projectConfiguredEdge.getConfiguredProperties());
      assertEquals(1, configuredProperties.size());

      currConfiguredProperty = configuredProperties.get(0);
      assertEquals("role", currConfiguredProperty.getPropertyName());
      assertEquals("STRING", currConfiguredProperty.getPropertyType());
      assertTrue(currConfiguredProperty.isIncludedInMigration());
      assertFalse(currConfiguredProperty.isMandatory());
      assertFalse(currConfiguredProperty.isReadOnly());
      assertFalse(currConfiguredProperty.isNotNull());
      currPropertyMapping = currConfiguredProperty.getPropertyMapping();
      assertNotNull(currPropertyMapping);
      assertEquals("hsqldb_EMPLOYEE_PROJECT", currPropertyMapping.getSourceName());
      assertEquals("ROLE", currPropertyMapping.getColumnName());
      assertEquals("VARCHAR", currPropertyMapping.getType());

      /**
       * HasDepartment edge check
       */

      assertEquals("HasDepartment", hasDepartmentConfiguredEdge.getName());

      // mapping check
      mappings = hasDepartmentConfiguredEdge.getMappings();
      assertEquals(1, mappings.size());
      edgeMapping = mappings.get(0);
      assertEquals(hasDepartmentConfiguredEdge, edgeMapping.getBelongingEdge());
      assertEquals("EMPLOYEE", edgeMapping.getFromTableName());
      assertEquals("DEPARTMENT", edgeMapping.getToTableName());
      assertEquals(1, edgeMapping.getFromColumns().size());
      assertEquals("DEPARTMENT", edgeMapping.getFromColumns().get(0));
      assertEquals(1, edgeMapping.getToColumns().size());
      assertEquals("ID", edgeMapping.getToColumns().get(0));
      assertEquals("direct", edgeMapping.getDirection());
      joinTableMapping = edgeMapping.getRepresentedJoinTableMapping();
      assertNull(joinTableMapping);

      // properties check
      configuredProperties = new ArrayList<OConfiguredProperty>(hasDepartmentConfiguredEdge.getConfiguredProperties());
      assertEquals(0, configuredProperties.size());

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

  @Test
  /**
   * Testing: - JSON building from OConfiguration (case 1)
   */

  public void test4() {

    OConfigurationHandler configurationHandler = new OConfigurationHandler(true);
    Connection connection = null;
    Statement st = null;

    try {

      /**
       * Graph model building
       */

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String departmentTableBuilding =
          "create memory table DEPARTMENT (ID varchar(256) not null, DEPARTMENT_NAME  varchar(256) not null,"
              + " LOCATION varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(departmentTableBuilding);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, SALARY double not null,"
          + " EMAIL varchar(256) not null, DEPARTMENT varchar(256) not null, primary key (ID),"
          + " foreign key (DEPARTMENT) references DEPARTMENT(ID))";
      st.execute(employeeTableBuilding);

      String projectTableBuilding = "create memory table PROJECT (ID varchar(256) not null, PROJECT_NAME  varchar(256),"
          + " DESCRIPTION varchar(256) not null, START_DATE date not null, EXPECTED_END_DATE date not null, primary key (ID))";
      st.execute(projectTableBuilding);

      String projectEmployeeTableBuilding =
          "create memory table EMPLOYEE_PROJECT (EMPLOYEE_ID  varchar(256)not null, PROJECT_ID varchar(256) not null,"
              + " ROLE varchar(256) not null, primary key (EMPLOYEE_ID, PROJECT_ID), foreign key (EMPLOYEE_ID) references EMPLOYEE(ID),"
              + " foreign key (PROJECT_ID) references PROJECT(ID))";
      st.execute(projectEmployeeTableBuilding);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null);
      this.mapper.buildSourceDatabaseSchema();
      this.mapper.buildGraphModel(new OJavaConventionNameResolver());
      this.mapper.performMany2ManyAggregation();

      OConfiguration configuredGraph = configurationHandler.buildConfigurationFromMapper(this.mapper);

      /**
       * Testing JSON building
       */

      ODocument inputConfigurationDoc = null;
      try {
        inputConfigurationDoc = OFileManager.buildJsonFromFile(this.config3);
      } catch (IOException e) {
        e.printStackTrace();
        fail();
      }

      ODocument configuredGraphDoc = configurationHandler.buildJSONDocFromConfiguration(configuredGraph);
      assertTrue(ODocumentComparator.areEquals(inputConfigurationDoc, configuredGraphDoc));

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

  @Test
  /**
   * Testing: - JSON building from OConfiguration (splitting case)
   */

  public void test5() {

    OConfigurationHandler configurationHandler = new OConfigurationHandler(true);
    Connection connection = null;
    Statement st = null;

    try {

      /**
       * Graph model building
       */

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

      ODocument config = OFileManager.buildJsonFromFile(this.config4);
      OConfiguration migrationConfig = configurationHandler.buildConfigurationFromJSONDoc(config);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, migrationConfig);
      this.mapper.buildSourceDatabaseSchema();
      this.mapper.buildGraphModel(new OJavaConventionNameResolver());
      this.mapper.applyImportConfiguration();
      this.mapper.performMany2ManyAggregation();

      OConfiguration configuredGraph = configurationHandler.buildConfigurationFromMapper(this.mapper);

      /**
       * Testing JSON building
       */

      ODocument inputConfigurationDoc = null;
      try {
        inputConfigurationDoc = OFileManager.buildJsonFromFile(this.config4);
      } catch (IOException e) {
        e.printStackTrace();
        fail();
      }

      ODocument configuredGraphDoc = configurationHandler.buildJSONDocFromConfiguration(configuredGraph);
      String input = inputConfigurationDoc.toJSON("");
      String configured = configuredGraphDoc.toJSON("");
      assertTrue(ODocumentComparator.areEquals(inputConfigurationDoc, configuredGraphDoc));

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
