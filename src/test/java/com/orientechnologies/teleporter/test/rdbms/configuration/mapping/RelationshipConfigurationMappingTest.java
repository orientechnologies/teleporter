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

import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
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
 * @email <g.ponzi--at--orientdb.com>
 */

public class RelationshipConfigurationMappingTest {

  private OER2GraphMapper    mapper;
  private OTeleporterContext context;
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

      ODocument config = OFileManager.buildJsonFromFile(this.configDirectEdgesPath);
      OConfigurationHandler configHandler = new OConfigurationHandler(false);
      OConfiguration migrationConfig = configHandler.buildConfigurationFromJSONDoc(config);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, migrationConfig);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());
      mapper.applyImportConfiguration();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);

       /*
       *  Testing built source db schema
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity projectEntity = mapper.getDataBaseSchema().getEntityByName("PROJECT");

      // entities check
      assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
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
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertEquals(1, projectEntity.getOutCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
      assertEquals(1, projectEntity.getInCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getInCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(1, projectEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> it = projectEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("EMPLOYEE", currentRelationship.getParentEntity().getName());
      assertEquals("PROJECT", currentRelationship.getForeignEntity().getName());
      assertEquals(employeeEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(projectEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = employeeEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT_MANAGER", projectEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("EMP_ID", employeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

      it = employeeEntity.getOutCanonicalRelationships().iterator();
      currentRelationship = it.next();
      assertEquals("PROJECT", currentRelationship.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentRelationship.getForeignEntity().getName());
      assertEquals(projectEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      it2 = projectEntity.getInCanonicalRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT", employeeEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", projectEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());


      /*
       *  Testing built graph model
       */
      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType projectVertexType = mapper.getGraphModel().getVertexTypeByName("Project");
      OEdgeType worksAtProjectEdgeType = mapper.getGraphModel().getEdgeTypeByName("WorksAtProject");
      OEdgeType hasManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasManager");

      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);

      // properties check
      assertEquals(4, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("empId"));
      assertEquals("empId", employeeVertexType.getPropertyByName("empId").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("empId").getOriginalType());
      assertEquals(1, employeeVertexType.getPropertyByName("empId").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("empId").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", employeeVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, employeeVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", employeeVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, employeeVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("project"));
      assertEquals("project", employeeVertexType.getPropertyByName("project").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("project").getOriginalType());
      assertEquals(4, employeeVertexType.getPropertyByName("project").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("project").isFromPrimaryKey());

      assertEquals(1, employeeVertexType.getOutEdgesType().size());
      assertEquals(worksAtProjectEdgeType, employeeVertexType.getOutEdgesType().get(0));
      assertEquals(1, employeeVertexType.getInEdgesType().size());
      assertEquals(hasManagerEdgeType, employeeVertexType.getInEdgesType().get(0));

      assertEquals(3, projectVertexType.getProperties().size());

      assertNotNull(projectVertexType.getPropertyByName("id"));
      assertEquals("id", projectVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, projectVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, projectVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("title"));
      assertEquals("title", projectVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, projectVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("projectManager"));
      assertEquals("projectManager", projectVertexType.getPropertyByName("projectManager").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("projectManager").getOriginalType());
      assertEquals(3, projectVertexType.getPropertyByName("projectManager").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("projectManager").isFromPrimaryKey());

      assertEquals(1, projectVertexType.getOutEdgesType().size());
      assertEquals(hasManagerEdgeType, projectVertexType.getOutEdgesType().get(0));
      assertEquals(1, projectVertexType.getInEdgesType().size());
      assertEquals(worksAtProjectEdgeType, projectVertexType.getInEdgesType().get(0));

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(worksAtProjectEdgeType);
      assertNotNull(hasManagerEdgeType);

      assertEquals("WorksAtProject", worksAtProjectEdgeType.getName());
      assertEquals(1, worksAtProjectEdgeType.getProperties().size());
      assertEquals("Project", worksAtProjectEdgeType.getInVertexType().getName());
      assertEquals(1, worksAtProjectEdgeType.getNumberRelationshipsRepresented());

      assertEquals(1, worksAtProjectEdgeType.getAllProperties().size());
      OModelProperty updatedOnProperty = worksAtProjectEdgeType.getPropertyByName("updatedOn");
      assertNotNull(updatedOnProperty);
      assertEquals("updatedOn", updatedOnProperty.getName());
      assertEquals(1, updatedOnProperty.getOrdinalPosition());
      assertEquals(false, updatedOnProperty.isFromPrimaryKey());
      assertEquals("DATE", updatedOnProperty.getOrientdbType());
      assertEquals(false, updatedOnProperty.isMandatory());
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
      assertEquals("DATE", updatedOnProperty.getOrientdbType());
      assertEquals(false, updatedOnProperty.isMandatory());
      assertEquals(false, updatedOnProperty.isReadOnly());
      assertEquals(false, updatedOnProperty.isNotNull());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(2, mapper.getVertexType2EVClassMappers().size());
      assertEquals(2, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
      OEVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
      assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
      assertEquals(employeeClassMapper.getEntity(), employeeEntity);
      assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

      assertEquals(4, employeeClassMapper.getAttribute2property().size());
      assertEquals(4, employeeClassMapper.getProperty2attribute().size());
      assertEquals("empId", employeeClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("firstName", employeeClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", employeeClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("project", employeeClassMapper.getAttribute2property().get("PROJECT"));
      assertEquals("EMP_ID", employeeClassMapper.getProperty2attribute().get("empId"));
      assertEquals("FIRST_NAME", employeeClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", employeeClassMapper.getProperty2attribute().get("lastName"));
      assertEquals("PROJECT", employeeClassMapper.getProperty2attribute().get("project"));

      assertEquals(1, mapper.getEVClassMappersByVertex(projectVertexType).size());
      OEVClassMapper projectEmployeeClassMapper = mapper.getEVClassMappersByVertex(projectVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(projectEntity).size());
      assertEquals(projectEmployeeClassMapper, mapper.getEVClassMappersByEntity(projectEntity).get(0));
      assertEquals(projectEmployeeClassMapper.getEntity(), projectEntity);
      assertEquals(projectEmployeeClassMapper.getVertexType(), projectVertexType);

      assertEquals(3, projectEmployeeClassMapper.getAttribute2property().size());
      assertEquals(3, projectEmployeeClassMapper.getProperty2attribute().size());
      assertEquals("id", projectEmployeeClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", projectEmployeeClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("projectManager", projectEmployeeClassMapper.getAttribute2property().get("PROJECT_MANAGER"));
      assertEquals("ID", projectEmployeeClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", projectEmployeeClassMapper.getProperty2attribute().get("title"));
      assertEquals("PROJECT_MANAGER", projectEmployeeClassMapper.getProperty2attribute().get("projectManager"));

      // Relationships-Edges Mapping

      Iterator<OCanonicalRelationship> itRelationships = employeeEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship worksAtRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      itRelationships = projectEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship hasManagerRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(hasManagerEdgeType, mapper.getRelationship2edgeType().get(hasManagerRelationship));
      assertEquals(worksAtProjectEdgeType, mapper.getRelationship2edgeType().get(worksAtRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(hasManagerEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(hasManagerEdgeType).contains(hasManagerRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(worksAtProjectEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(worksAtProjectEdgeType).contains(worksAtRelationship));

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

      parentTableBuilding = "alter table EMPLOYEE add foreign key (PROJECT) references PROJECT(ID)";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      ODocument config = OFileManager.buildJsonFromFile(this.configInverseEdgesPath);
      OConfigurationHandler configHandler = new OConfigurationHandler(false);
      OConfiguration migrationConfig = configHandler.buildConfigurationFromJSONDoc(config);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, migrationConfig);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());
      mapper.applyImportConfiguration();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity projectEntity = mapper.getDataBaseSchema().getEntityByName("PROJECT");

      // entities check
      assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
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
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertEquals(1, projectEntity.getOutCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
      assertEquals(1, projectEntity.getInCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getInCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(1, projectEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> it = projectEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("EMPLOYEE", currentRelationship.getParentEntity().getName());
      assertEquals("PROJECT", currentRelationship.getForeignEntity().getName());
      assertEquals(employeeEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(projectEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = employeeEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT_MANAGER", projectEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("EMP_ID", employeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

      it = employeeEntity.getOutCanonicalRelationships().iterator();
      currentRelationship = it.next();
      assertEquals("PROJECT", currentRelationship.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentRelationship.getForeignEntity().getName());
      assertEquals(projectEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      it2 = projectEntity.getInCanonicalRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT", employeeEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", projectEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());


      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType projectVertexType = mapper.getGraphModel().getVertexTypeByName("Project");
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
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("empId").getOriginalType());
      assertEquals(1, employeeVertexType.getPropertyByName("empId").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("empId").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", employeeVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, employeeVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", employeeVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, employeeVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("project"));
      assertEquals("project", employeeVertexType.getPropertyByName("project").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("project").getOriginalType());
      assertEquals(4, employeeVertexType.getPropertyByName("project").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("project").isFromPrimaryKey());

      assertEquals(0, employeeVertexType.getOutEdgesType().size());
      assertEquals(2, employeeVertexType.getInEdgesType().size());
      assertEquals(hasEmployeeEdgeType, employeeVertexType.getInEdgesType().get(1));
      assertEquals(hasProjectManagerEdgeType, employeeVertexType.getInEdgesType().get(0));

      assertEquals(3, projectVertexType.getProperties().size());

      assertNotNull(projectVertexType.getPropertyByName("id"));
      assertEquals("id", projectVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, projectVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, projectVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("title"));
      assertEquals("title", projectVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, projectVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("projectManager"));
      assertEquals("projectManager", projectVertexType.getPropertyByName("projectManager").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("projectManager").getOriginalType());
      assertEquals(3, projectVertexType.getPropertyByName("projectManager").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("projectManager").isFromPrimaryKey());

      assertEquals(2, projectVertexType.getOutEdgesType().size());
      assertEquals(hasEmployeeEdgeType, projectVertexType.getOutEdgesType().get(1));
      assertEquals(hasProjectManagerEdgeType, projectVertexType.getOutEdgesType().get(0));
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
      assertEquals("DATE", updatedOnProperty.getOrientdbType());
      assertEquals(false, updatedOnProperty.isMandatory());
      assertEquals(false, updatedOnProperty.isReadOnly());
      assertEquals(false, updatedOnProperty.isNotNull());

      assertEquals("HasProjectManager", hasProjectManagerEdgeType.getName());
      assertEquals(0, hasProjectManagerEdgeType.getProperties().size());
      assertEquals("Employee", hasProjectManagerEdgeType.getInVertexType().getName());
      assertEquals(1, hasProjectManagerEdgeType.getNumberRelationshipsRepresented());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(2, mapper.getVertexType2EVClassMappers().size());
      assertEquals(2, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
      OEVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
      assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
      assertEquals(employeeClassMapper.getEntity(), employeeEntity);
      assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

      assertEquals(4, employeeClassMapper.getAttribute2property().size());
      assertEquals(4, employeeClassMapper.getProperty2attribute().size());
      assertEquals("empId", employeeClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("firstName", employeeClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", employeeClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("project", employeeClassMapper.getAttribute2property().get("PROJECT"));
      assertEquals("EMP_ID", employeeClassMapper.getProperty2attribute().get("empId"));
      assertEquals("FIRST_NAME", employeeClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", employeeClassMapper.getProperty2attribute().get("lastName"));
      assertEquals("PROJECT", employeeClassMapper.getProperty2attribute().get("project"));

      assertEquals(1, mapper.getEVClassMappersByVertex(projectVertexType).size());
      OEVClassMapper projectEmployeeClassMapper = mapper.getEVClassMappersByVertex(projectVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(projectEntity).size());
      assertEquals(projectEmployeeClassMapper, mapper.getEVClassMappersByEntity(projectEntity).get(0));
      assertEquals(projectEmployeeClassMapper.getEntity(), projectEntity);
      assertEquals(projectEmployeeClassMapper.getVertexType(), projectVertexType);

      assertEquals(3, projectEmployeeClassMapper.getAttribute2property().size());
      assertEquals(3, projectEmployeeClassMapper.getProperty2attribute().size());
      assertEquals("id", projectEmployeeClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", projectEmployeeClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("projectManager", projectEmployeeClassMapper.getAttribute2property().get("PROJECT_MANAGER"));
      assertEquals("ID", projectEmployeeClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", projectEmployeeClassMapper.getProperty2attribute().get("title"));
      assertEquals("PROJECT_MANAGER", projectEmployeeClassMapper.getProperty2attribute().get("projectManager"));

      // Relationships-Edges Mapping

      Iterator<OCanonicalRelationship> itRelationships = employeeEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship projectRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      itRelationships = projectEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship hasProjectManagerRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(hasProjectManagerEdgeType, mapper.getRelationship2edgeType().get(hasProjectManagerRelationship));
      assertEquals(hasEmployeeEdgeType, mapper.getRelationship2edgeType().get(projectRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(hasProjectManagerEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(hasProjectManagerEdgeType).contains(hasProjectManagerRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(hasEmployeeEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(hasEmployeeEdgeType).contains(projectRelationship));

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
   *    - year (type DATE): mandatory=F, readOnly=F, notNull=F.
   */

  public void test3() {

    this.context.setExecutionStrategy("naive-aggregate");
    Connection connection = null;
    Statement st = null;

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

      ODocument config = OFileManager.buildJsonFromFile(this.configJoinTableDirectEdgesPath);
      OConfigurationHandler configHandler = new OConfigurationHandler(true);
      OConfiguration migrationConfig = configHandler.buildConfigurationFromJSONDoc(config);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, migrationConfig);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());
      mapper.applyImportConfiguration();


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfEntities);
      assertEquals(3, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity actorEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR");
      OEntity filmEntity = mapper.getDataBaseSchema().getEntityByName("FILM");
      OEntity actorFilmEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR_FILM");

      // entities check
      assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(actorEntity);
      assertNotNull(filmEntity);
      assertNotNull(actorFilmEntity);

      // attributes check
      assertEquals(3, actorEntity.getAttributes().size());

      assertNotNull(actorEntity.getAttributeByName("ID"));
      assertEquals("ID", actorEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, actorEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(actorEntity.getAttributeByName("FIRST_NAME"));
      assertEquals("FIRST_NAME", actorEntity.getAttributeByName("FIRST_NAME").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("FIRST_NAME").getDataType());
      assertEquals(2, actorEntity.getAttributeByName("FIRST_NAME").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("FIRST_NAME").getBelongingEntity().getName());

      assertNotNull(actorEntity.getAttributeByName("LAST_NAME"));
      assertEquals("LAST_NAME", actorEntity.getAttributeByName("LAST_NAME").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("LAST_NAME").getDataType());
      assertEquals(3, actorEntity.getAttributeByName("LAST_NAME").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("LAST_NAME").getBelongingEntity().getName());

      assertEquals(3, filmEntity.getAttributes().size());

      assertNotNull(filmEntity.getAttributeByName("ID"));
      assertEquals("ID", filmEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, filmEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(filmEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", filmEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(2, filmEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(filmEntity.getAttributeByName("CATEGORY"));
      assertEquals("CATEGORY", filmEntity.getAttributeByName("CATEGORY").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("CATEGORY").getDataType());
      assertEquals(3, filmEntity.getAttributeByName("CATEGORY").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("CATEGORY").getBelongingEntity().getName());

      assertEquals(3, actorFilmEntity.getAttributes().size());

      assertNotNull(actorFilmEntity.getAttributeByName("ACTOR_ID"));
      assertEquals("ACTOR_ID", actorFilmEntity.getAttributeByName("ACTOR_ID").getName());
      assertEquals("VARCHAR", actorFilmEntity.getAttributeByName("ACTOR_ID").getDataType());
      assertEquals(1, actorFilmEntity.getAttributeByName("ACTOR_ID").getOrdinalPosition());
      assertEquals("ACTOR_FILM", actorFilmEntity.getAttributeByName("ACTOR_ID").getBelongingEntity().getName());

      assertNotNull(actorFilmEntity.getAttributeByName("FILM_ID"));
      assertEquals("FILM_ID", actorFilmEntity.getAttributeByName("FILM_ID").getName());
      assertEquals("VARCHAR", actorFilmEntity.getAttributeByName("FILM_ID").getDataType());
      assertEquals(2, actorFilmEntity.getAttributeByName("FILM_ID").getOrdinalPosition());
      assertEquals("ACTOR_FILM", actorFilmEntity.getAttributeByName("FILM_ID").getBelongingEntity().getName());

      assertNotNull(actorFilmEntity.getAttributeByName("PAYMENT"));
      assertEquals("PAYMENT", actorFilmEntity.getAttributeByName("PAYMENT").getName());
      assertEquals("INTEGER", actorFilmEntity.getAttributeByName("PAYMENT").getDataType());
      assertEquals(3, actorFilmEntity.getAttributeByName("PAYMENT").getOrdinalPosition());
      assertEquals("ACTOR_FILM", actorFilmEntity.getAttributeByName("PAYMENT").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertEquals(0, filmEntity.getOutCanonicalRelationships().size());
      assertEquals(0, actorEntity.getOutCanonicalRelationships().size());
      assertEquals(2, actorFilmEntity.getOutCanonicalRelationships().size());
      assertEquals(1, filmEntity.getInCanonicalRelationships().size());
      assertEquals(1, actorEntity.getInCanonicalRelationships().size());
      assertEquals(0, actorFilmEntity.getInCanonicalRelationships().size());
      assertEquals(0, actorEntity.getForeignKeys().size());
      assertEquals(0, filmEntity.getForeignKeys().size());
      assertEquals(2, actorFilmEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> it = actorFilmEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("ACTOR", currentRelationship.getParentEntity().getName());
      assertEquals("ACTOR_FILM", currentRelationship.getForeignEntity().getName());
      assertEquals(actorEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(actorFilmEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = actorEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("ACTOR_ID", actorFilmEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", actorEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      currentRelationship = it.next();
      assertEquals("FILM", currentRelationship.getParentEntity().getName());
      assertEquals("ACTOR_FILM", currentRelationship.getForeignEntity().getName());
      assertEquals(filmEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(actorFilmEntity.getForeignKeys().get(1), currentRelationship.getForeignKey());

      it2 = filmEntity.getInCanonicalRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("FILM_ID", actorFilmEntity.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", filmEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

       /*
       *  Testing built graph model
       */

      OVertexType actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      OVertexType filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      OVertexType actorFilmVertexType = mapper.getGraphModel().getVertexTypeByName("ActorFilm");
      OEdgeType performsLeftEdgeType = mapper.getGraphModel().getEdgeTypeByName("Performs-left");
      OEdgeType performsRightEdgeType = mapper.getGraphModel().getEdgeTypeByName("Performs-right");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNotNull(actorFilmVertexType);

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", actorVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, actorVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", actorVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, actorVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertEquals(0, actorVertexType.getOutEdgesType().size());
      assertEquals(1, actorVertexType.getInEdgesType().size());
      assertEquals(performsLeftEdgeType, actorVertexType.getInEdgesType().get(0));

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("category"));
      assertEquals("category", filmVertexType.getPropertyByName("category").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("category").getOriginalType());
      assertEquals(3, filmVertexType.getPropertyByName("category").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("category").isFromPrimaryKey());

      assertEquals(0, filmVertexType.getOutEdgesType().size());
      assertEquals(1, filmVertexType.getInEdgesType().size());
      assertEquals(performsRightEdgeType, filmVertexType.getInEdgesType().get(0));

      assertEquals(3, actorFilmVertexType.getProperties().size());

      assertNotNull(actorFilmVertexType.getPropertyByName("actorId"));
      assertEquals("actorId", actorFilmVertexType.getPropertyByName("actorId").getName());
      assertEquals("VARCHAR", actorFilmVertexType.getPropertyByName("actorId").getOriginalType());
      assertEquals(1, actorFilmVertexType.getPropertyByName("actorId").getOrdinalPosition());
      assertEquals(true, actorFilmVertexType.getPropertyByName("actorId").isFromPrimaryKey());

      assertNotNull(actorFilmVertexType.getPropertyByName("filmId"));
      assertEquals("filmId", actorFilmVertexType.getPropertyByName("filmId").getName());
      assertEquals("VARCHAR", actorFilmVertexType.getPropertyByName("filmId").getOriginalType());
      assertEquals(2, actorFilmVertexType.getPropertyByName("filmId").getOrdinalPosition());
      assertEquals(true, actorFilmVertexType.getPropertyByName("filmId").isFromPrimaryKey());

      assertNotNull(actorFilmVertexType.getPropertyByName("payment"));
      assertEquals("payment", actorFilmVertexType.getPropertyByName("payment").getName());
      assertEquals("INTEGER", actorFilmVertexType.getPropertyByName("payment").getOriginalType());
      assertEquals(3, actorFilmVertexType.getPropertyByName("payment").getOrdinalPosition());
      assertEquals(false, actorFilmVertexType.getPropertyByName("payment").isFromPrimaryKey());

      assertEquals(2, actorFilmVertexType.getOutEdgesType().size());
      assertEquals(0, actorFilmVertexType.getInEdgesType().size());
      assertEquals(performsLeftEdgeType, actorFilmVertexType.getOutEdgesType().get(0));
      assertEquals(performsRightEdgeType, actorFilmVertexType.getOutEdgesType().get(1));

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(performsLeftEdgeType);
      assertNotNull(performsRightEdgeType);

      assertEquals("Performs-left", performsLeftEdgeType.getName());
      assertEquals(1, performsLeftEdgeType.getProperties().size());
      assertEquals("Actor", performsLeftEdgeType.getInVertexType().getName());
      assertEquals(1, performsLeftEdgeType.getNumberRelationshipsRepresented());

      OModelProperty yearProperty = performsLeftEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(1, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      assertEquals("Performs-right", performsRightEdgeType.getName());
      assertEquals(1, performsRightEdgeType.getProperties().size());
      assertEquals("Film", performsRightEdgeType.getInVertexType().getName());
      assertEquals(1, performsRightEdgeType.getNumberRelationshipsRepresented());

      yearProperty = performsRightEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(1, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2EVClassMappers().size());
      assertEquals(3, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(actorVertexType).size());
      OEVClassMapper actorClassMapper = mapper.getEVClassMappersByVertex(actorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorEntity).size());
      assertEquals(actorClassMapper, mapper.getEVClassMappersByEntity(actorEntity).get(0));
      assertEquals(actorClassMapper.getEntity(), actorEntity);
      assertEquals(actorClassMapper.getVertexType(), actorVertexType);

      assertEquals(3, actorClassMapper.getAttribute2property().size());
      assertEquals(3, actorClassMapper.getProperty2attribute().size());
      assertEquals("id", actorClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", actorClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", actorClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", actorClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", actorClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", actorClassMapper.getProperty2attribute().get("lastName"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmVertexType).size());
      OEVClassMapper filmClassMapper = mapper.getEVClassMappersByVertex(filmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmEntity).size());
      assertEquals(filmClassMapper, mapper.getEVClassMappersByEntity(filmEntity).get(0));
      assertEquals(filmClassMapper.getEntity(), filmEntity);
      assertEquals(filmClassMapper.getVertexType(), filmVertexType);

      assertEquals(3, filmClassMapper.getAttribute2property().size());
      assertEquals(3, filmClassMapper.getProperty2attribute().size());
      assertEquals("id", filmClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", filmClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("category", filmClassMapper.getAttribute2property().get("CATEGORY"));
      assertEquals("ID", filmClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", filmClassMapper.getProperty2attribute().get("title"));
      assertEquals("CATEGORY", filmClassMapper.getProperty2attribute().get("category"));

      assertEquals(1, mapper.getEVClassMappersByVertex(actorFilmVertexType).size());
      OEVClassMapper actorFilmClassMapper = mapper.getEVClassMappersByVertex(actorFilmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorFilmEntity).size());
      assertEquals(actorFilmClassMapper, mapper.getEVClassMappersByEntity(actorFilmEntity).get(0));
      assertEquals(actorFilmClassMapper.getEntity(), actorFilmEntity);
      assertEquals(actorFilmClassMapper.getVertexType(), actorFilmVertexType);

      assertEquals(3, actorFilmClassMapper.getAttribute2property().size());
      assertEquals(3, actorFilmClassMapper.getProperty2attribute().size());
      assertEquals("actorId", actorFilmClassMapper.getAttribute2property().get("ACTOR_ID"));
      assertEquals("filmId", actorFilmClassMapper.getAttribute2property().get("FILM_ID"));
      assertEquals("payment", actorFilmClassMapper.getAttribute2property().get("PAYMENT"));
      assertEquals("ACTOR_ID", actorFilmClassMapper.getProperty2attribute().get("actorId"));
      assertEquals("FILM_ID", actorFilmClassMapper.getProperty2attribute().get("filmId"));
      assertEquals("PAYMENT", actorFilmClassMapper.getProperty2attribute().get("payment"));

      // Relationships-Edges Mapping

      Iterator<OCanonicalRelationship> itRelationships = actorFilmEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship performsLeftRelationship = itRelationships.next();
      OCanonicalRelationship performsRightRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(performsLeftEdgeType, mapper.getRelationship2edgeType().get(performsLeftRelationship));
      assertEquals(performsRightEdgeType, mapper.getRelationship2edgeType().get(performsRightRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(performsLeftEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsLeftEdgeType).contains(performsLeftRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(performsRightEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsRightEdgeType).contains(performsRightRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

      /**
       * performing aggregations
       */
      mapper.performAggregations();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */

      actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      OEdgeType performsEdgeType = mapper.getGraphModel().getEdgeTypeByName("Performs");

      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNull(mapper.getGraphModel().getVertexTypeByName("ActorFilm"));
      assertNull(mapper.getGraphModel().getEdgeTypeByName("Performs-left"));
      assertNull(mapper.getGraphModel().getEdgeTypeByName("Performs-right"));

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", actorVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, actorVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", actorVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, actorVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertEquals(1, actorVertexType.getOutEdgesType().size());
      assertEquals(0, actorVertexType.getInEdgesType().size());
      assertEquals(performsEdgeType, actorVertexType.getOutEdgesType().get(0));

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("category"));
      assertEquals("category", filmVertexType.getPropertyByName("category").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("category").getOriginalType());
      assertEquals(3, filmVertexType.getPropertyByName("category").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("category").isFromPrimaryKey());

      assertEquals(0, filmVertexType.getOutEdgesType().size());
      assertEquals(1, filmVertexType.getInEdgesType().size());
      assertEquals(performsEdgeType, filmVertexType.getInEdgesType().get(0));

      // edges check
      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(performsEdgeType);

      assertEquals("Performs", performsEdgeType.getName());
      assertEquals(2, performsEdgeType.getProperties().size());
      assertEquals("Film", performsEdgeType.getInVertexType().getName());
      assertEquals(1, performsEdgeType.getNumberRelationshipsRepresented());

      assertEquals(2, performsEdgeType.getAllProperties().size());
       OModelProperty paymentProperty = performsEdgeType.getPropertyByName("payment");
      assertNotNull(paymentProperty);
      assertEquals("payment", paymentProperty.getName());
      assertEquals(1, paymentProperty.getOrdinalPosition());
      assertEquals(false, paymentProperty.isFromPrimaryKey());
      assertEquals("INTEGER", paymentProperty.getOriginalType());
      assertNull(paymentProperty.isMandatory());
      assertNull(paymentProperty.isReadOnly());
      assertNull(paymentProperty.isNotNull());

      yearProperty = performsEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(2, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2EVClassMappers().size());
      assertEquals(3, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(actorVertexType).size());
      actorClassMapper = mapper.getEVClassMappersByVertex(actorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorEntity).size());
      assertEquals(actorClassMapper, mapper.getEVClassMappersByEntity(actorEntity).get(0));
      assertEquals(actorClassMapper.getEntity(), actorEntity);
      assertEquals(actorClassMapper.getVertexType(), actorVertexType);

      assertEquals(3, actorClassMapper.getAttribute2property().size());
      assertEquals(3, actorClassMapper.getProperty2attribute().size());
      assertEquals("id", actorClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", actorClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", actorClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", actorClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", actorClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", actorClassMapper.getProperty2attribute().get("lastName"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmVertexType).size());
      filmClassMapper = mapper.getEVClassMappersByVertex(filmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmEntity).size());
      assertEquals(filmClassMapper, mapper.getEVClassMappersByEntity(filmEntity).get(0));
      assertEquals(filmClassMapper.getEntity(), filmEntity);
      assertEquals(filmClassMapper.getVertexType(), filmVertexType);

      assertEquals(3, filmClassMapper.getAttribute2property().size());
      assertEquals(3, filmClassMapper.getProperty2attribute().size());
      assertEquals("id", filmClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", filmClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("category", filmClassMapper.getAttribute2property().get("CATEGORY"));
      assertEquals("ID", filmClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", filmClassMapper.getProperty2attribute().get("title"));
      assertEquals("CATEGORY", filmClassMapper.getProperty2attribute().get("category"));

      assertEquals(1, mapper.getEVClassMappersByVertex(actorFilmVertexType).size());
      actorFilmClassMapper = mapper.getEVClassMappersByVertex(actorFilmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorFilmEntity).size());
      assertEquals(actorFilmClassMapper, mapper.getEVClassMappersByEntity(actorFilmEntity).get(0));
      assertEquals(actorFilmClassMapper.getEntity(), actorFilmEntity);
      assertEquals(actorFilmClassMapper.getVertexType(), actorFilmVertexType);

      assertEquals(3, actorFilmClassMapper.getAttribute2property().size());
      assertEquals(3, actorFilmClassMapper.getProperty2attribute().size());
      assertEquals("actorId", actorFilmClassMapper.getAttribute2property().get("ACTOR_ID"));
      assertEquals("filmId", actorFilmClassMapper.getAttribute2property().get("FILM_ID"));
      assertEquals("payment", actorFilmClassMapper.getAttribute2property().get("PAYMENT"));
      assertEquals("ACTOR_ID", actorFilmClassMapper.getProperty2attribute().get("actorId"));
      assertEquals("FILM_ID", actorFilmClassMapper.getProperty2attribute().get("filmId"));
      assertEquals("PAYMENT", actorFilmClassMapper.getProperty2attribute().get("payment"));

      // Relationships-Edges Mapping

      itRelationships = actorFilmEntity.getOutCanonicalRelationships().iterator();
      performsLeftRelationship = itRelationships.next();
      performsRightRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(performsLeftEdgeType, mapper.getRelationship2edgeType().get(performsLeftRelationship));
      assertEquals(performsRightEdgeType, mapper.getRelationship2edgeType().get(performsRightRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(performsLeftEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsLeftEdgeType).contains(performsLeftRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(performsRightEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsRightEdgeType).contains(performsRightRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(1, mapper.getJoinVertex2aggregatorEdges().size());
      assertTrue(mapper.getJoinVertex2aggregatorEdges().containsKey(actorFilmVertexType));
      assertEquals(performsEdgeType, mapper.getJoinVertex2aggregatorEdges().get(actorFilmVertexType).getEdgeType());
      assertEquals("Actor", mapper.getJoinVertex2aggregatorEdges().get(actorFilmVertexType).getOutVertexClassName());
      assertEquals("Film", mapper.getJoinVertex2aggregatorEdges().get(actorFilmVertexType).getInVertexClassName());

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
          + " FILM_ID varchar(256) not null, PAYMENT integer, primary key (ACTOR_ID, FILM_ID))";
      st.execute(actorFilmTableBuilding);

      ODocument config = OFileManager.buildJsonFromFile(this.configJoinTableInverseEdgesPath);
      OConfigurationHandler configHandler = new OConfigurationHandler(true);
      OConfiguration migrationConfig = configHandler.buildConfigurationFromJSONDoc(config);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, migrationConfig);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());
      mapper.applyImportConfiguration();


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfEntities);
      assertEquals(3, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity actorEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR");
      OEntity filmEntity = mapper.getDataBaseSchema().getEntityByName("FILM");
      OEntity filmActorEntity = mapper.getDataBaseSchema().getEntityByName("FILM_ACTOR");

      // entities check
      assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(actorEntity);
      assertNotNull(filmEntity);
      assertNotNull(filmActorEntity);

      // attributes check
      assertEquals(3, actorEntity.getAttributes().size());

      assertNotNull(actorEntity.getAttributeByName("ID"));
      assertEquals("ID", actorEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, actorEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(actorEntity.getAttributeByName("FIRST_NAME"));
      assertEquals("FIRST_NAME", actorEntity.getAttributeByName("FIRST_NAME").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("FIRST_NAME").getDataType());
      assertEquals(2, actorEntity.getAttributeByName("FIRST_NAME").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("FIRST_NAME").getBelongingEntity().getName());

      assertNotNull(actorEntity.getAttributeByName("LAST_NAME"));
      assertEquals("LAST_NAME", actorEntity.getAttributeByName("LAST_NAME").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("LAST_NAME").getDataType());
      assertEquals(3, actorEntity.getAttributeByName("LAST_NAME").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("LAST_NAME").getBelongingEntity().getName());

      assertEquals(3, filmEntity.getAttributes().size());

      assertNotNull(filmEntity.getAttributeByName("ID"));
      assertEquals("ID", filmEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, filmEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(filmEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", filmEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(2, filmEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(filmEntity.getAttributeByName("CATEGORY"));
      assertEquals("CATEGORY", filmEntity.getAttributeByName("CATEGORY").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("CATEGORY").getDataType());
      assertEquals(3, filmEntity.getAttributeByName("CATEGORY").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("CATEGORY").getBelongingEntity().getName());

      assertEquals(3, filmActorEntity.getAttributes().size());

      assertNotNull(filmActorEntity.getAttributeByName("ACTOR_ID"));
      assertEquals("ACTOR_ID", filmActorEntity.getAttributeByName("ACTOR_ID").getName());
      assertEquals("VARCHAR", filmActorEntity.getAttributeByName("ACTOR_ID").getDataType());
      assertEquals(1, filmActorEntity.getAttributeByName("ACTOR_ID").getOrdinalPosition());
      assertEquals("FILM_ACTOR", filmActorEntity.getAttributeByName("ACTOR_ID").getBelongingEntity().getName());

      assertNotNull(filmActorEntity.getAttributeByName("FILM_ID"));
      assertEquals("FILM_ID", filmActorEntity.getAttributeByName("FILM_ID").getName());
      assertEquals("VARCHAR", filmActorEntity.getAttributeByName("FILM_ID").getDataType());
      assertEquals(2, filmActorEntity.getAttributeByName("FILM_ID").getOrdinalPosition());
      assertEquals("FILM_ACTOR", filmActorEntity.getAttributeByName("FILM_ID").getBelongingEntity().getName());

      assertNotNull(filmActorEntity.getAttributeByName("PAYMENT"));
      assertEquals("PAYMENT", filmActorEntity.getAttributeByName("PAYMENT").getName());
      assertEquals("INTEGER", filmActorEntity.getAttributeByName("PAYMENT").getDataType());
      assertEquals(3, filmActorEntity.getAttributeByName("PAYMENT").getOrdinalPosition());
      assertEquals("FILM_ACTOR", filmActorEntity.getAttributeByName("PAYMENT").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertEquals(0, filmEntity.getOutCanonicalRelationships().size());
      assertEquals(0, actorEntity.getOutCanonicalRelationships().size());
      assertEquals(2, filmActorEntity.getOutCanonicalRelationships().size());
      assertEquals(1, filmEntity.getInCanonicalRelationships().size());
      assertEquals(1, actorEntity.getInCanonicalRelationships().size());
      assertEquals(0, filmActorEntity.getInCanonicalRelationships().size());
      assertEquals(0, actorEntity.getForeignKeys().size());
      assertEquals(0, filmEntity.getForeignKeys().size());
      assertEquals(2, filmActorEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> it = filmActorEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("FILM", currentRelationship.getParentEntity().getName());
      assertEquals("FILM_ACTOR", currentRelationship.getForeignEntity().getName());
      assertEquals(filmEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(filmActorEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = filmEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("FILM_ID", filmActorEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", filmEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      currentRelationship = it.next();

      assertEquals("ACTOR", currentRelationship.getParentEntity().getName());
      assertEquals("FILM_ACTOR", currentRelationship.getForeignEntity().getName());
      assertEquals(actorEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(filmActorEntity.getForeignKeys().get(1), currentRelationship.getForeignKey());

      it2 = actorEntity.getInCanonicalRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("ACTOR_ID", filmActorEntity.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", actorEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

       /*
       *  Testing built graph model
       */

      OVertexType actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      OVertexType filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      OVertexType filmActorVertexType = mapper.getGraphModel().getVertexTypeByName("FilmActor");
      OEdgeType performsLeftEdgeType = mapper.getGraphModel().getEdgeTypeByName("Performs-left");
      OEdgeType performsRightEdgeType = mapper.getGraphModel().getEdgeTypeByName("Performs-right");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNotNull(filmActorVertexType);

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", actorVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, actorVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", actorVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, actorVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertEquals(0, actorVertexType.getOutEdgesType().size());
      assertEquals(1, actorVertexType.getInEdgesType().size());
      assertEquals(performsRightEdgeType, actorVertexType.getInEdgesType().get(0));

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("category"));
      assertEquals("category", filmVertexType.getPropertyByName("category").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("category").getOriginalType());
      assertEquals(3, filmVertexType.getPropertyByName("category").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("category").isFromPrimaryKey());

      assertEquals(0, filmVertexType.getOutEdgesType().size());
      assertEquals(1, filmVertexType.getInEdgesType().size());
      assertEquals(performsLeftEdgeType, filmVertexType.getInEdgesType().get(0));

      assertEquals(3, filmActorVertexType.getProperties().size());

      assertNotNull(filmActorVertexType.getPropertyByName("actorId"));
      assertEquals("actorId", filmActorVertexType.getPropertyByName("actorId").getName());
      assertEquals("VARCHAR", filmActorVertexType.getPropertyByName("actorId").getOriginalType());
      assertEquals(1, filmActorVertexType.getPropertyByName("actorId").getOrdinalPosition());
      assertEquals(true, filmActorVertexType.getPropertyByName("actorId").isFromPrimaryKey());

      assertNotNull(filmActorVertexType.getPropertyByName("filmId"));
      assertEquals("filmId", filmActorVertexType.getPropertyByName("filmId").getName());
      assertEquals("VARCHAR", filmActorVertexType.getPropertyByName("filmId").getOriginalType());
      assertEquals(2, filmActorVertexType.getPropertyByName("filmId").getOrdinalPosition());
      assertEquals(true, filmActorVertexType.getPropertyByName("filmId").isFromPrimaryKey());

      assertNotNull(filmActorVertexType.getPropertyByName("payment"));
      assertEquals("payment", filmActorVertexType.getPropertyByName("payment").getName());
      assertEquals("INTEGER", filmActorVertexType.getPropertyByName("payment").getOriginalType());
      assertEquals(3, filmActorVertexType.getPropertyByName("payment").getOrdinalPosition());
      assertEquals(false, filmActorVertexType.getPropertyByName("payment").isFromPrimaryKey());

      assertEquals(2, filmActorVertexType.getOutEdgesType().size());
      assertEquals(0, filmActorVertexType.getInEdgesType().size());
      assertEquals(performsLeftEdgeType, filmActorVertexType.getOutEdgesType().get(0));
      assertEquals(performsRightEdgeType, filmActorVertexType.getOutEdgesType().get(1));

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(performsLeftEdgeType);
      assertNotNull(performsRightEdgeType);

      assertEquals("Performs-left", performsLeftEdgeType.getName());
      assertEquals(1, performsLeftEdgeType.getProperties().size());
      assertEquals("Film", performsLeftEdgeType.getInVertexType().getName());
      assertEquals(1, performsLeftEdgeType.getNumberRelationshipsRepresented());

      OModelProperty yearProperty = performsLeftEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(1, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      assertEquals("Performs-right", performsRightEdgeType.getName());
      assertEquals(1, performsRightEdgeType.getProperties().size());
      assertEquals("Actor", performsRightEdgeType.getInVertexType().getName());
      assertEquals(1, performsRightEdgeType.getNumberRelationshipsRepresented());

      yearProperty = performsRightEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(1, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2EVClassMappers().size());
      assertEquals(3, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(actorVertexType).size());
      OEVClassMapper actorClassMapper = mapper.getEVClassMappersByVertex(actorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorEntity).size());
      assertEquals(actorClassMapper, mapper.getEVClassMappersByEntity(actorEntity).get(0));
      assertEquals(actorClassMapper.getEntity(), actorEntity);
      assertEquals(actorClassMapper.getVertexType(), actorVertexType);

      assertEquals(3, actorClassMapper.getAttribute2property().size());
      assertEquals(3, actorClassMapper.getProperty2attribute().size());
      assertEquals("id", actorClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", actorClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", actorClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", actorClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", actorClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", actorClassMapper.getProperty2attribute().get("lastName"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmVertexType).size());
      OEVClassMapper filmClassMapper = mapper.getEVClassMappersByVertex(filmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmEntity).size());
      assertEquals(filmClassMapper, mapper.getEVClassMappersByEntity(filmEntity).get(0));
      assertEquals(filmClassMapper.getEntity(), filmEntity);
      assertEquals(filmClassMapper.getVertexType(), filmVertexType);

      assertEquals(3, filmClassMapper.getAttribute2property().size());
      assertEquals(3, filmClassMapper.getProperty2attribute().size());
      assertEquals("id", filmClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", filmClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("category", filmClassMapper.getAttribute2property().get("CATEGORY"));
      assertEquals("ID", filmClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", filmClassMapper.getProperty2attribute().get("title"));
      assertEquals("CATEGORY", filmClassMapper.getProperty2attribute().get("category"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmActorVertexType).size());
      OEVClassMapper filmActorClassMapper = mapper.getEVClassMappersByVertex(filmActorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmActorEntity).size());
      assertEquals(filmActorClassMapper, mapper.getEVClassMappersByEntity(filmActorEntity).get(0));
      assertEquals(filmActorClassMapper.getEntity(), filmActorEntity);
      assertEquals(filmActorClassMapper.getVertexType(), filmActorVertexType);

      assertEquals(3, filmActorClassMapper.getAttribute2property().size());
      assertEquals(3, filmActorClassMapper.getProperty2attribute().size());
      assertEquals("actorId", filmActorClassMapper.getAttribute2property().get("ACTOR_ID"));
      assertEquals("filmId", filmActorClassMapper.getAttribute2property().get("FILM_ID"));
      assertEquals("payment", filmActorClassMapper.getAttribute2property().get("PAYMENT"));
      assertEquals("ACTOR_ID", filmActorClassMapper.getProperty2attribute().get("actorId"));
      assertEquals("FILM_ID", filmActorClassMapper.getProperty2attribute().get("filmId"));
      assertEquals("PAYMENT", filmActorClassMapper.getProperty2attribute().get("payment"));

      // Relationships-Edges Mapping

      Iterator<OCanonicalRelationship> itRelationships = filmActorEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship performsLeftRelationship = itRelationships.next();
      OCanonicalRelationship performsRightRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(performsLeftEdgeType, mapper.getRelationship2edgeType().get(performsLeftRelationship));
      assertEquals(performsRightEdgeType, mapper.getRelationship2edgeType().get(performsRightRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(performsLeftEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsLeftEdgeType).contains(performsLeftRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(performsRightEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsRightEdgeType).contains(performsRightRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

      /**
       * performing aggregations
       */
      mapper.performAggregations();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */

      actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      OEdgeType performsEdgeType = mapper.getGraphModel().getEdgeTypeByName("Performs");

      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNull(mapper.getGraphModel().getVertexTypeByName("FilmActor"));
      assertNull(mapper.getGraphModel().getEdgeTypeByName("Performs-left"));
      assertNull(mapper.getGraphModel().getEdgeTypeByName("Performs-right"));

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", actorVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, actorVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", actorVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, actorVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertEquals(1, actorVertexType.getOutEdgesType().size());
      assertEquals(0, actorVertexType.getInEdgesType().size());
      assertEquals(performsEdgeType, actorVertexType.getOutEdgesType().get(0));

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("category"));
      assertEquals("category", filmVertexType.getPropertyByName("category").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("category").getOriginalType());
      assertEquals(3, filmVertexType.getPropertyByName("category").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("category").isFromPrimaryKey());

      assertEquals(0, filmVertexType.getOutEdgesType().size());
      assertEquals(1, filmVertexType.getInEdgesType().size());
      assertEquals(performsEdgeType, filmVertexType.getInEdgesType().get(0));

      // edges check
      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(performsEdgeType);

      assertEquals("Performs", performsEdgeType.getName());
      assertEquals(2, performsEdgeType.getProperties().size());
      assertEquals("Film", performsEdgeType.getInVertexType().getName());
      assertEquals(1, performsEdgeType.getNumberRelationshipsRepresented());

      assertEquals(2, performsEdgeType.getAllProperties().size());
      OModelProperty paymentProperty = performsEdgeType.getPropertyByName("payment");
      assertNotNull(paymentProperty);
      assertEquals("payment", paymentProperty.getName());
      assertEquals(1, paymentProperty.getOrdinalPosition());
      assertEquals(false, paymentProperty.isFromPrimaryKey());
      assertEquals("INTEGER", paymentProperty.getOriginalType());
      assertNull(paymentProperty.isMandatory());
      assertNull(paymentProperty.isReadOnly());
      assertNull(paymentProperty.isNotNull());

      yearProperty = performsEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(2, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

            /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2EVClassMappers().size());
      assertEquals(3, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(actorVertexType).size());
      actorClassMapper = mapper.getEVClassMappersByVertex(actorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorEntity).size());
      assertEquals(actorClassMapper, mapper.getEVClassMappersByEntity(actorEntity).get(0));
      assertEquals(actorClassMapper.getEntity(), actorEntity);
      assertEquals(actorClassMapper.getVertexType(), actorVertexType);

      assertEquals(3, actorClassMapper.getAttribute2property().size());
      assertEquals(3, actorClassMapper.getProperty2attribute().size());
      assertEquals("id", actorClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", actorClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", actorClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", actorClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", actorClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", actorClassMapper.getProperty2attribute().get("lastName"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmVertexType).size());
      filmClassMapper = mapper.getEVClassMappersByVertex(filmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmEntity).size());
      assertEquals(filmClassMapper, mapper.getEVClassMappersByEntity(filmEntity).get(0));
      assertEquals(filmClassMapper.getEntity(), filmEntity);
      assertEquals(filmClassMapper.getVertexType(), filmVertexType);

      assertEquals(3, filmClassMapper.getAttribute2property().size());
      assertEquals(3, filmClassMapper.getProperty2attribute().size());
      assertEquals("id", filmClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", filmClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("category", filmClassMapper.getAttribute2property().get("CATEGORY"));
      assertEquals("ID", filmClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", filmClassMapper.getProperty2attribute().get("title"));
      assertEquals("CATEGORY", filmClassMapper.getProperty2attribute().get("category"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmActorVertexType).size());
      filmActorClassMapper = mapper.getEVClassMappersByVertex(filmActorVertexType).get(0);

      assertEquals(filmActorClassMapper, mapper.getEVClassMappersByEntity(filmActorEntity).get(0));
      assertEquals(filmActorClassMapper.getEntity(), filmActorEntity);
      assertEquals(filmActorClassMapper.getVertexType(), filmActorVertexType);

      assertEquals(3, filmActorClassMapper.getAttribute2property().size());
      assertEquals(3, filmActorClassMapper.getProperty2attribute().size());
      assertEquals("actorId", filmActorClassMapper.getAttribute2property().get("ACTOR_ID"));
      assertEquals("filmId", filmActorClassMapper.getAttribute2property().get("FILM_ID"));
      assertEquals("payment", filmActorClassMapper.getAttribute2property().get("PAYMENT"));
      assertEquals("ACTOR_ID", filmActorClassMapper.getProperty2attribute().get("actorId"));
      assertEquals("FILM_ID", filmActorClassMapper.getProperty2attribute().get("filmId"));
      assertEquals("PAYMENT", filmActorClassMapper.getProperty2attribute().get("payment"));

      // Relationships-Edges Mapping

      itRelationships = filmActorEntity.getOutCanonicalRelationships().iterator();
      performsLeftRelationship = itRelationships.next();
      performsRightRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(performsLeftEdgeType, mapper.getRelationship2edgeType().get(performsLeftRelationship));
      assertEquals(performsRightEdgeType, mapper.getRelationship2edgeType().get(performsRightRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(performsLeftEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsLeftEdgeType).contains(performsLeftRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(performsRightEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsRightEdgeType).contains(performsRightRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(1, mapper.getJoinVertex2aggregatorEdges().size());
      assertTrue(mapper.getJoinVertex2aggregatorEdges().containsKey(filmActorVertexType));
      assertEquals(performsEdgeType, mapper.getJoinVertex2aggregatorEdges().get(filmActorVertexType).getEdgeType());
      assertEquals("Actor", mapper.getJoinVertex2aggregatorEdges().get(filmActorVertexType).getOutVertexClassName());
      assertEquals("Film", mapper.getJoinVertex2aggregatorEdges().get(filmActorVertexType).getInVertexClassName());

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

      ODocument config = OFileManager.buildJsonFromFile(this.configJoinTableDirectEdgesPath);
      OConfigurationHandler configHandler = new OConfigurationHandler(true);
      OConfiguration migrationConfig = configHandler.buildConfigurationFromJSONDoc(config);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, migrationConfig);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());
      mapper.applyImportConfiguration();


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfEntities);
      assertEquals(3, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity actorEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR");
      OEntity filmEntity = mapper.getDataBaseSchema().getEntityByName("FILM");
      OEntity actorFilmEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR_FILM");

      // entities check
      assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(actorEntity);
      assertNotNull(filmEntity);
      assertNotNull(actorFilmEntity);

      // attributes check
      assertEquals(3, actorEntity.getAttributes().size());

      assertNotNull(actorEntity.getAttributeByName("ID"));
      assertEquals("ID", actorEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, actorEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(actorEntity.getAttributeByName("FIRST_NAME"));
      assertEquals("FIRST_NAME", actorEntity.getAttributeByName("FIRST_NAME").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("FIRST_NAME").getDataType());
      assertEquals(2, actorEntity.getAttributeByName("FIRST_NAME").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("FIRST_NAME").getBelongingEntity().getName());

      assertNotNull(actorEntity.getAttributeByName("LAST_NAME"));
      assertEquals("LAST_NAME", actorEntity.getAttributeByName("LAST_NAME").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("LAST_NAME").getDataType());
      assertEquals(3, actorEntity.getAttributeByName("LAST_NAME").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("LAST_NAME").getBelongingEntity().getName());

      assertEquals(3, filmEntity.getAttributes().size());

      assertNotNull(filmEntity.getAttributeByName("ID"));
      assertEquals("ID", filmEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, filmEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(filmEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", filmEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(2, filmEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(filmEntity.getAttributeByName("CATEGORY"));
      assertEquals("CATEGORY", filmEntity.getAttributeByName("CATEGORY").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("CATEGORY").getDataType());
      assertEquals(3, filmEntity.getAttributeByName("CATEGORY").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("CATEGORY").getBelongingEntity().getName());

      assertEquals(3, actorFilmEntity.getAttributes().size());

      assertNotNull(actorFilmEntity.getAttributeByName("ACTOR_ID"));
      assertEquals("ACTOR_ID", actorFilmEntity.getAttributeByName("ACTOR_ID").getName());
      assertEquals("VARCHAR", actorFilmEntity.getAttributeByName("ACTOR_ID").getDataType());
      assertEquals(1, actorFilmEntity.getAttributeByName("ACTOR_ID").getOrdinalPosition());
      assertEquals("ACTOR_FILM", actorFilmEntity.getAttributeByName("ACTOR_ID").getBelongingEntity().getName());

      assertNotNull(actorFilmEntity.getAttributeByName("FILM_ID"));
      assertEquals("FILM_ID", actorFilmEntity.getAttributeByName("FILM_ID").getName());
      assertEquals("VARCHAR", actorFilmEntity.getAttributeByName("FILM_ID").getDataType());
      assertEquals(2, actorFilmEntity.getAttributeByName("FILM_ID").getOrdinalPosition());
      assertEquals("ACTOR_FILM", actorFilmEntity.getAttributeByName("FILM_ID").getBelongingEntity().getName());

      assertNotNull(actorFilmEntity.getAttributeByName("PAYMENT"));
      assertEquals("PAYMENT", actorFilmEntity.getAttributeByName("PAYMENT").getName());
      assertEquals("INTEGER", actorFilmEntity.getAttributeByName("PAYMENT").getDataType());
      assertEquals(3, actorFilmEntity.getAttributeByName("PAYMENT").getOrdinalPosition());
      assertEquals("ACTOR_FILM", actorFilmEntity.getAttributeByName("PAYMENT").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertEquals(0, filmEntity.getOutCanonicalRelationships().size());
      assertEquals(0, actorEntity.getOutCanonicalRelationships().size());
      assertEquals(2, actorFilmEntity.getOutCanonicalRelationships().size());
      assertEquals(1, filmEntity.getInCanonicalRelationships().size());
      assertEquals(1, actorEntity.getInCanonicalRelationships().size());
      assertEquals(0, actorFilmEntity.getInCanonicalRelationships().size());
      assertEquals(0, actorEntity.getForeignKeys().size());
      assertEquals(0, filmEntity.getForeignKeys().size());
      assertEquals(2, actorFilmEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> it = actorFilmEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("ACTOR", currentRelationship.getParentEntity().getName());
      assertEquals("ACTOR_FILM", currentRelationship.getForeignEntity().getName());
      assertEquals(actorEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(actorFilmEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = actorEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("ACTOR_ID", actorFilmEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", actorEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      currentRelationship = it.next();
      assertEquals("FILM", currentRelationship.getParentEntity().getName());
      assertEquals("ACTOR_FILM", currentRelationship.getForeignEntity().getName());
      assertEquals(filmEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(actorFilmEntity.getForeignKeys().get(1), currentRelationship.getForeignKey());

      it2 = filmEntity.getInCanonicalRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("FILM_ID", actorFilmEntity.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", filmEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

       /*
       *  Testing built graph model
       */

      OVertexType actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      OVertexType filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      OVertexType actorFilmVertexType = mapper.getGraphModel().getVertexTypeByName("ActorFilm");
      OEdgeType performsLeftEdgeType = mapper.getGraphModel().getEdgeTypeByName("Performs-left");
      OEdgeType performsRightEdgeType = mapper.getGraphModel().getEdgeTypeByName("Performs-right");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNotNull(actorFilmVertexType);

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", actorVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, actorVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", actorVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, actorVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertEquals(0, actorVertexType.getOutEdgesType().size());
      assertEquals(1, actorVertexType.getInEdgesType().size());
      assertEquals(performsLeftEdgeType, actorVertexType.getInEdgesType().get(0));

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("category"));
      assertEquals("category", filmVertexType.getPropertyByName("category").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("category").getOriginalType());
      assertEquals(3, filmVertexType.getPropertyByName("category").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("category").isFromPrimaryKey());

      assertEquals(0, filmVertexType.getOutEdgesType().size());
      assertEquals(1, filmVertexType.getInEdgesType().size());
      assertEquals(performsRightEdgeType, filmVertexType.getInEdgesType().get(0));

      assertEquals(3, actorFilmVertexType.getProperties().size());

      assertNotNull(actorFilmVertexType.getPropertyByName("actorId"));
      assertEquals("actorId", actorFilmVertexType.getPropertyByName("actorId").getName());
      assertEquals("VARCHAR", actorFilmVertexType.getPropertyByName("actorId").getOriginalType());
      assertEquals(1, actorFilmVertexType.getPropertyByName("actorId").getOrdinalPosition());
      assertEquals(true, actorFilmVertexType.getPropertyByName("actorId").isFromPrimaryKey());

      assertNotNull(actorFilmVertexType.getPropertyByName("filmId"));
      assertEquals("filmId", actorFilmVertexType.getPropertyByName("filmId").getName());
      assertEquals("VARCHAR", actorFilmVertexType.getPropertyByName("filmId").getOriginalType());
      assertEquals(2, actorFilmVertexType.getPropertyByName("filmId").getOrdinalPosition());
      assertEquals(true, actorFilmVertexType.getPropertyByName("filmId").isFromPrimaryKey());

      assertNotNull(actorFilmVertexType.getPropertyByName("payment"));
      assertEquals("payment", actorFilmVertexType.getPropertyByName("payment").getName());
      assertEquals("INTEGER", actorFilmVertexType.getPropertyByName("payment").getOriginalType());
      assertEquals(3, actorFilmVertexType.getPropertyByName("payment").getOrdinalPosition());
      assertEquals(false, actorFilmVertexType.getPropertyByName("payment").isFromPrimaryKey());

      assertEquals(2, actorFilmVertexType.getOutEdgesType().size());
      assertEquals(0, actorFilmVertexType.getInEdgesType().size());
      assertEquals(performsLeftEdgeType, actorFilmVertexType.getOutEdgesType().get(0));
      assertEquals(performsRightEdgeType, actorFilmVertexType.getOutEdgesType().get(1));

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(performsLeftEdgeType);
      assertNotNull(performsRightEdgeType);

      assertEquals("Performs-left", performsLeftEdgeType.getName());
      assertEquals(1, performsLeftEdgeType.getProperties().size());
      assertEquals("Actor", performsLeftEdgeType.getInVertexType().getName());
      assertEquals(1, performsLeftEdgeType.getNumberRelationshipsRepresented());

      OModelProperty yearProperty = performsLeftEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(1, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      assertEquals("Performs-right", performsRightEdgeType.getName());
      assertEquals(1, performsRightEdgeType.getProperties().size());
      assertEquals("Film", performsRightEdgeType.getInVertexType().getName());
      assertEquals(1, performsRightEdgeType.getNumberRelationshipsRepresented());

      yearProperty = performsRightEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(1, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2EVClassMappers().size());
      assertEquals(3, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(actorVertexType).size());
      OEVClassMapper actorClassMapper = mapper.getEVClassMappersByVertex(actorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorEntity).size());
      assertEquals(actorClassMapper, mapper.getEVClassMappersByEntity(actorEntity).get(0));
      assertEquals(actorClassMapper.getEntity(), actorEntity);
      assertEquals(actorClassMapper.getVertexType(), actorVertexType);

      assertEquals(3, actorClassMapper.getAttribute2property().size());
      assertEquals(3, actorClassMapper.getProperty2attribute().size());
      assertEquals("id", actorClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", actorClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", actorClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", actorClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", actorClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", actorClassMapper.getProperty2attribute().get("lastName"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmVertexType).size());
      OEVClassMapper filmClassMapper = mapper.getEVClassMappersByVertex(filmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmEntity).size());
      assertEquals(filmClassMapper, mapper.getEVClassMappersByEntity(filmEntity).get(0));
      assertEquals(filmClassMapper.getEntity(), filmEntity);
      assertEquals(filmClassMapper.getVertexType(), filmVertexType);

      assertEquals(3, filmClassMapper.getAttribute2property().size());
      assertEquals(3, filmClassMapper.getProperty2attribute().size());
      assertEquals("id", filmClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", filmClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("category", filmClassMapper.getAttribute2property().get("CATEGORY"));
      assertEquals("ID", filmClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", filmClassMapper.getProperty2attribute().get("title"));
      assertEquals("CATEGORY", filmClassMapper.getProperty2attribute().get("category"));

      assertEquals(1, mapper.getEVClassMappersByVertex(actorFilmVertexType).size());
      OEVClassMapper actorFilmClassMapper = mapper.getEVClassMappersByVertex(actorFilmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorFilmEntity).size());
      assertEquals(actorFilmClassMapper, mapper.getEVClassMappersByEntity(actorFilmEntity).get(0));
      assertEquals(actorFilmClassMapper.getEntity(), actorFilmEntity);
      assertEquals(actorFilmClassMapper.getVertexType(), actorFilmVertexType);

      assertEquals(3, actorFilmClassMapper.getAttribute2property().size());
      assertEquals(3, actorFilmClassMapper.getProperty2attribute().size());
      assertEquals("actorId", actorFilmClassMapper.getAttribute2property().get("ACTOR_ID"));
      assertEquals("filmId", actorFilmClassMapper.getAttribute2property().get("FILM_ID"));
      assertEquals("payment", actorFilmClassMapper.getAttribute2property().get("PAYMENT"));
      assertEquals("ACTOR_ID", actorFilmClassMapper.getProperty2attribute().get("actorId"));
      assertEquals("FILM_ID", actorFilmClassMapper.getProperty2attribute().get("filmId"));
      assertEquals("PAYMENT", actorFilmClassMapper.getProperty2attribute().get("payment"));

      // Relationships-Edges Mapping

      Iterator<OCanonicalRelationship> itRelationships = actorFilmEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship performsLeftRelationship = itRelationships.next();
      OCanonicalRelationship performsRightRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(performsLeftEdgeType, mapper.getRelationship2edgeType().get(performsLeftRelationship));
      assertEquals(performsRightEdgeType, mapper.getRelationship2edgeType().get(performsRightRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(performsLeftEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsLeftEdgeType).contains(performsLeftRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(performsRightEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsRightEdgeType).contains(performsRightRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

      /**
       * performing aggregations
       */
      mapper.performAggregations();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */

      actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      OEdgeType performsEdgeType = mapper.getGraphModel().getEdgeTypeByName("Performs");

      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNull(mapper.getGraphModel().getVertexTypeByName("ActorFilm"));
      assertNull(mapper.getGraphModel().getEdgeTypeByName("Performs-left"));
      assertNull(mapper.getGraphModel().getEdgeTypeByName("Performs-right"));

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", actorVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, actorVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", actorVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, actorVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertEquals(1, actorVertexType.getOutEdgesType().size());
      assertEquals(0, actorVertexType.getInEdgesType().size());
      assertEquals(performsEdgeType, actorVertexType.getOutEdgesType().get(0));

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("category"));
      assertEquals("category", filmVertexType.getPropertyByName("category").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("category").getOriginalType());
      assertEquals(3, filmVertexType.getPropertyByName("category").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("category").isFromPrimaryKey());

      assertEquals(0, filmVertexType.getOutEdgesType().size());
      assertEquals(1, filmVertexType.getInEdgesType().size());
      assertEquals(performsEdgeType, filmVertexType.getInEdgesType().get(0));

      // edges check
      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(performsEdgeType);

      assertEquals("Performs", performsEdgeType.getName());
      assertEquals(2, performsEdgeType.getProperties().size());
      assertEquals("Film", performsEdgeType.getInVertexType().getName());
      assertEquals(1, performsEdgeType.getNumberRelationshipsRepresented());

      assertEquals(2, performsEdgeType.getAllProperties().size());
      OModelProperty paymentProperty = performsEdgeType.getPropertyByName("payment");
      assertNotNull(paymentProperty);
      assertEquals("payment", paymentProperty.getName());
      assertEquals(1, paymentProperty.getOrdinalPosition());
      assertEquals(false, paymentProperty.isFromPrimaryKey());
      assertEquals("INTEGER", paymentProperty.getOriginalType());
      assertNull(paymentProperty.isMandatory());
      assertNull(paymentProperty.isReadOnly());
      assertNull(paymentProperty.isNotNull());

      yearProperty = performsEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(2, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2EVClassMappers().size());
      assertEquals(3, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(actorVertexType).size());
      actorClassMapper = mapper.getEVClassMappersByVertex(actorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorEntity).size());
      assertEquals(actorClassMapper, mapper.getEVClassMappersByEntity(actorEntity).get(0));
      assertEquals(actorClassMapper.getEntity(), actorEntity);
      assertEquals(actorClassMapper.getVertexType(), actorVertexType);

      assertEquals(3, actorClassMapper.getAttribute2property().size());
      assertEquals(3, actorClassMapper.getProperty2attribute().size());
      assertEquals("id", actorClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", actorClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", actorClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", actorClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", actorClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", actorClassMapper.getProperty2attribute().get("lastName"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmVertexType).size());
      filmClassMapper = mapper.getEVClassMappersByVertex(filmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmEntity).size());
      assertEquals(filmClassMapper, mapper.getEVClassMappersByEntity(filmEntity).get(0));
      assertEquals(filmClassMapper.getEntity(), filmEntity);
      assertEquals(filmClassMapper.getVertexType(), filmVertexType);

      assertEquals(3, filmClassMapper.getAttribute2property().size());
      assertEquals(3, filmClassMapper.getProperty2attribute().size());
      assertEquals("id", filmClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", filmClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("category", filmClassMapper.getAttribute2property().get("CATEGORY"));
      assertEquals("ID", filmClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", filmClassMapper.getProperty2attribute().get("title"));
      assertEquals("CATEGORY", filmClassMapper.getProperty2attribute().get("category"));

      assertEquals(1, mapper.getEVClassMappersByVertex(actorFilmVertexType).size());
      actorFilmClassMapper = mapper.getEVClassMappersByVertex(actorFilmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorFilmEntity).size());
      assertEquals(actorFilmClassMapper, mapper.getEVClassMappersByEntity(actorFilmEntity).get(0));
      assertEquals(actorFilmClassMapper.getEntity(), actorFilmEntity);
      assertEquals(actorFilmClassMapper.getVertexType(), actorFilmVertexType);

      assertEquals(3, actorFilmClassMapper.getAttribute2property().size());
      assertEquals(3, actorFilmClassMapper.getProperty2attribute().size());
      assertEquals("actorId", actorFilmClassMapper.getAttribute2property().get("ACTOR_ID"));
      assertEquals("filmId", actorFilmClassMapper.getAttribute2property().get("FILM_ID"));
      assertEquals("payment", actorFilmClassMapper.getAttribute2property().get("PAYMENT"));
      assertEquals("ACTOR_ID", actorFilmClassMapper.getProperty2attribute().get("actorId"));
      assertEquals("FILM_ID", actorFilmClassMapper.getProperty2attribute().get("filmId"));
      assertEquals("PAYMENT", actorFilmClassMapper.getProperty2attribute().get("payment"));

      // Relationships-Edges Mapping

      itRelationships = actorFilmEntity.getOutCanonicalRelationships().iterator();
      performsLeftRelationship = itRelationships.next();
      performsRightRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(performsLeftEdgeType, mapper.getRelationship2edgeType().get(performsLeftRelationship));
      assertEquals(performsRightEdgeType, mapper.getRelationship2edgeType().get(performsRightRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(performsLeftEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsLeftEdgeType).contains(performsLeftRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(performsRightEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(performsRightEdgeType).contains(performsRightRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(1, mapper.getJoinVertex2aggregatorEdges().size());
      assertTrue(mapper.getJoinVertex2aggregatorEdges().containsKey(actorFilmVertexType));
      assertEquals(performsEdgeType, mapper.getJoinVertex2aggregatorEdges().get(actorFilmVertexType).getEdgeType());
      assertEquals("Actor", mapper.getJoinVertex2aggregatorEdges().get(actorFilmVertexType).getOutVertexClassName());
      assertEquals("Film", mapper.getJoinVertex2aggregatorEdges().get(actorFilmVertexType).getInVertexClassName());

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

      ODocument config = OFileManager.buildJsonFromFile(this.configJoinTableInverseEdgesPath2);
      OConfigurationHandler configHandler = new OConfigurationHandler(true);
      OConfiguration migrationConfig = configHandler.buildConfigurationFromJSONDoc(config);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, migrationConfig);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());
      mapper.applyImportConfiguration();


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfEntities);
      assertEquals(3, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity actorEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR");
      OEntity filmEntity = mapper.getDataBaseSchema().getEntityByName("FILM");
      OEntity filmActorEntity = mapper.getDataBaseSchema().getEntityByName("FILM_ACTOR");

      // entities check
      assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(actorEntity);
      assertNotNull(filmEntity);
      assertNotNull(filmActorEntity);

      // attributes check
      assertEquals(3, actorEntity.getAttributes().size());

      assertNotNull(actorEntity.getAttributeByName("ID"));
      assertEquals("ID", actorEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, actorEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(actorEntity.getAttributeByName("FIRST_NAME"));
      assertEquals("FIRST_NAME", actorEntity.getAttributeByName("FIRST_NAME").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("FIRST_NAME").getDataType());
      assertEquals(2, actorEntity.getAttributeByName("FIRST_NAME").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("FIRST_NAME").getBelongingEntity().getName());

      assertNotNull(actorEntity.getAttributeByName("LAST_NAME"));
      assertEquals("LAST_NAME", actorEntity.getAttributeByName("LAST_NAME").getName());
      assertEquals("VARCHAR", actorEntity.getAttributeByName("LAST_NAME").getDataType());
      assertEquals(3, actorEntity.getAttributeByName("LAST_NAME").getOrdinalPosition());
      assertEquals("ACTOR", actorEntity.getAttributeByName("LAST_NAME").getBelongingEntity().getName());

      assertEquals(3, filmEntity.getAttributes().size());

      assertNotNull(filmEntity.getAttributeByName("ID"));
      assertEquals("ID", filmEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, filmEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(filmEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", filmEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(2, filmEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(filmEntity.getAttributeByName("CATEGORY"));
      assertEquals("CATEGORY", filmEntity.getAttributeByName("CATEGORY").getName());
      assertEquals("VARCHAR", filmEntity.getAttributeByName("CATEGORY").getDataType());
      assertEquals(3, filmEntity.getAttributeByName("CATEGORY").getOrdinalPosition());
      assertEquals("FILM", filmEntity.getAttributeByName("CATEGORY").getBelongingEntity().getName());

      assertEquals(3, filmActorEntity.getAttributes().size());

      assertNotNull(filmActorEntity.getAttributeByName("ACTOR_ID"));
      assertEquals("ACTOR_ID", filmActorEntity.getAttributeByName("ACTOR_ID").getName());
      assertEquals("VARCHAR", filmActorEntity.getAttributeByName("ACTOR_ID").getDataType());
      assertEquals(1, filmActorEntity.getAttributeByName("ACTOR_ID").getOrdinalPosition());
      assertEquals("FILM_ACTOR", filmActorEntity.getAttributeByName("ACTOR_ID").getBelongingEntity().getName());

      assertNotNull(filmActorEntity.getAttributeByName("FILM_ID"));
      assertEquals("FILM_ID", filmActorEntity.getAttributeByName("FILM_ID").getName());
      assertEquals("VARCHAR", filmActorEntity.getAttributeByName("FILM_ID").getDataType());
      assertEquals(2, filmActorEntity.getAttributeByName("FILM_ID").getOrdinalPosition());
      assertEquals("FILM_ACTOR", filmActorEntity.getAttributeByName("FILM_ID").getBelongingEntity().getName());

      assertNotNull(filmActorEntity.getAttributeByName("PAYMENT"));
      assertEquals("PAYMENT", filmActorEntity.getAttributeByName("PAYMENT").getName());
      assertEquals("INTEGER", filmActorEntity.getAttributeByName("PAYMENT").getDataType());
      assertEquals(3, filmActorEntity.getAttributeByName("PAYMENT").getOrdinalPosition());
      assertEquals("FILM_ACTOR", filmActorEntity.getAttributeByName("PAYMENT").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertEquals(0, filmEntity.getOutCanonicalRelationships().size());
      assertEquals(0, actorEntity.getOutCanonicalRelationships().size());
      assertEquals(2, filmActorEntity.getOutCanonicalRelationships().size());
      assertEquals(1, filmEntity.getInCanonicalRelationships().size());
      assertEquals(1, actorEntity.getInCanonicalRelationships().size());
      assertEquals(0, filmActorEntity.getInCanonicalRelationships().size());
      assertEquals(0, actorEntity.getForeignKeys().size());
      assertEquals(0, filmEntity.getForeignKeys().size());
      assertEquals(2, filmActorEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> it = filmActorEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("ACTOR", currentRelationship.getParentEntity().getName());
      assertEquals("FILM_ACTOR", currentRelationship.getForeignEntity().getName());
      assertEquals(actorEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(filmActorEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = actorEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("ACTOR_ID", filmActorEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", actorEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      currentRelationship = it.next();

      assertEquals("FILM", currentRelationship.getParentEntity().getName());
      assertEquals("FILM_ACTOR", currentRelationship.getForeignEntity().getName());
      assertEquals(filmEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(filmActorEntity.getForeignKeys().get(1), currentRelationship.getForeignKey());

      it2 = filmEntity.getInCanonicalRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("FILM_ID", filmActorEntity.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", filmEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

       /*
       *  Testing built graph model
       */

      OVertexType actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      OVertexType filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      OVertexType filmActorVertexType = mapper.getGraphModel().getVertexTypeByName("FilmActor");
      OEdgeType featuresLeftEdgeType = mapper.getGraphModel().getEdgeTypeByName("Features-left");
      OEdgeType featuresRightEdgeType = mapper.getGraphModel().getEdgeTypeByName("Features-right");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNotNull(filmActorVertexType);

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", actorVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, actorVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", actorVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, actorVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertEquals(0, actorVertexType.getOutEdgesType().size());
      assertEquals(1, actorVertexType.getInEdgesType().size());
      assertEquals(featuresLeftEdgeType, actorVertexType.getInEdgesType().get(0));

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("category"));
      assertEquals("category", filmVertexType.getPropertyByName("category").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("category").getOriginalType());
      assertEquals(3, filmVertexType.getPropertyByName("category").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("category").isFromPrimaryKey());

      assertEquals(0, filmVertexType.getOutEdgesType().size());
      assertEquals(1, filmVertexType.getInEdgesType().size());
      assertEquals(featuresRightEdgeType, filmVertexType.getInEdgesType().get(0));

      assertEquals(3, filmActorVertexType.getProperties().size());

      assertNotNull(filmActorVertexType.getPropertyByName("actorId"));
      assertEquals("actorId", filmActorVertexType.getPropertyByName("actorId").getName());
      assertEquals("VARCHAR", filmActorVertexType.getPropertyByName("actorId").getOriginalType());
      assertEquals(1, filmActorVertexType.getPropertyByName("actorId").getOrdinalPosition());
      assertEquals(true, filmActorVertexType.getPropertyByName("actorId").isFromPrimaryKey());

      assertNotNull(filmActorVertexType.getPropertyByName("filmId"));
      assertEquals("filmId", filmActorVertexType.getPropertyByName("filmId").getName());
      assertEquals("VARCHAR", filmActorVertexType.getPropertyByName("filmId").getOriginalType());
      assertEquals(2, filmActorVertexType.getPropertyByName("filmId").getOrdinalPosition());
      assertEquals(true, filmActorVertexType.getPropertyByName("filmId").isFromPrimaryKey());

      assertNotNull(filmActorVertexType.getPropertyByName("payment"));
      assertEquals("payment", filmActorVertexType.getPropertyByName("payment").getName());
      assertEquals("INTEGER", filmActorVertexType.getPropertyByName("payment").getOriginalType());
      assertEquals(3, filmActorVertexType.getPropertyByName("payment").getOrdinalPosition());
      assertEquals(false, filmActorVertexType.getPropertyByName("payment").isFromPrimaryKey());

      assertEquals(2, filmActorVertexType.getOutEdgesType().size());
      assertEquals(0, filmActorVertexType.getInEdgesType().size());
      assertEquals(featuresLeftEdgeType, filmActorVertexType.getOutEdgesType().get(0));
      assertEquals(featuresRightEdgeType, filmActorVertexType.getOutEdgesType().get(1));

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(featuresLeftEdgeType);
      assertNotNull(featuresRightEdgeType);

      assertEquals("Features-left", featuresLeftEdgeType.getName());
      assertEquals(1, featuresLeftEdgeType.getProperties().size());
      assertEquals("Actor", featuresLeftEdgeType.getInVertexType().getName());
      assertEquals(1, featuresLeftEdgeType.getNumberRelationshipsRepresented());

      OModelProperty yearProperty = featuresLeftEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(1, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      assertEquals("Features-right", featuresRightEdgeType.getName());
      assertEquals(1, featuresRightEdgeType.getProperties().size());
      assertEquals("Film", featuresRightEdgeType.getInVertexType().getName());
      assertEquals(1, featuresRightEdgeType.getNumberRelationshipsRepresented());

      yearProperty = featuresRightEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(1, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2EVClassMappers().size());
      assertEquals(3, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(actorVertexType).size());
      OEVClassMapper actorClassMapper = mapper.getEVClassMappersByVertex(actorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorEntity).size());
      assertEquals(actorClassMapper, mapper.getEVClassMappersByEntity(actorEntity).get(0));
      assertEquals(actorClassMapper.getEntity(), actorEntity);
      assertEquals(actorClassMapper.getVertexType(), actorVertexType);

      assertEquals(3, actorClassMapper.getAttribute2property().size());
      assertEquals(3, actorClassMapper.getProperty2attribute().size());
      assertEquals("id", actorClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", actorClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", actorClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", actorClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", actorClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", actorClassMapper.getProperty2attribute().get("lastName"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmVertexType).size());
      OEVClassMapper filmClassMapper = mapper.getEVClassMappersByVertex(filmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmEntity).size());
      assertEquals(filmClassMapper, mapper.getEVClassMappersByEntity(filmEntity).get(0));
      assertEquals(filmClassMapper.getEntity(), filmEntity);
      assertEquals(filmClassMapper.getVertexType(), filmVertexType);

      assertEquals(3, filmClassMapper.getAttribute2property().size());
      assertEquals(3, filmClassMapper.getProperty2attribute().size());
      assertEquals("id", filmClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", filmClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("category", filmClassMapper.getAttribute2property().get("CATEGORY"));
      assertEquals("ID", filmClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", filmClassMapper.getProperty2attribute().get("title"));
      assertEquals("CATEGORY", filmClassMapper.getProperty2attribute().get("category"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmActorVertexType).size());
      OEVClassMapper actorFilmClassMapper = mapper.getEVClassMappersByVertex(filmActorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmActorEntity).size());
      assertEquals(actorFilmClassMapper, mapper.getEVClassMappersByEntity(filmActorEntity).get(0));
      assertEquals(actorFilmClassMapper.getEntity(), filmActorEntity);
      assertEquals(actorFilmClassMapper.getVertexType(), filmActorVertexType);

      assertEquals(3, actorFilmClassMapper.getAttribute2property().size());
      assertEquals(3, actorFilmClassMapper.getProperty2attribute().size());
      assertEquals("actorId", actorFilmClassMapper.getAttribute2property().get("ACTOR_ID"));
      assertEquals("filmId", actorFilmClassMapper.getAttribute2property().get("FILM_ID"));
      assertEquals("payment", actorFilmClassMapper.getAttribute2property().get("PAYMENT"));
      assertEquals("ACTOR_ID", actorFilmClassMapper.getProperty2attribute().get("actorId"));
      assertEquals("FILM_ID", actorFilmClassMapper.getProperty2attribute().get("filmId"));
      assertEquals("PAYMENT", actorFilmClassMapper.getProperty2attribute().get("payment"));

      // Relationships-Edges Mapping

      Iterator<OCanonicalRelationship> itRelationships = filmActorEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship performsLeftRelationship = itRelationships.next();
      OCanonicalRelationship performsRightRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(featuresLeftEdgeType, mapper.getRelationship2edgeType().get(performsLeftRelationship));
      assertEquals(featuresRightEdgeType, mapper.getRelationship2edgeType().get(performsRightRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(featuresLeftEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(featuresLeftEdgeType).contains(performsLeftRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(featuresRightEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(featuresRightEdgeType).contains(performsRightRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

      /**
       * performing aggregations
       */
      mapper.performAggregations();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */

      actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      OEdgeType featuresEdgeType = mapper.getGraphModel().getEdgeTypeByName("Features");

      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNull(mapper.getGraphModel().getVertexTypeByName("FilmActor"));
      assertNull(mapper.getGraphModel().getEdgeTypeByName("Features-left"));
      assertNull(mapper.getGraphModel().getEdgeTypeByName("Features-right"));

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("firstName"));
      assertEquals("firstName", actorVertexType.getPropertyByName("firstName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("firstName").getOriginalType());
      assertEquals(2, actorVertexType.getPropertyByName("firstName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("firstName").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("lastName"));
      assertEquals("lastName", actorVertexType.getPropertyByName("lastName").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("lastName").getOriginalType());
      assertEquals(3, actorVertexType.getPropertyByName("lastName").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("lastName").isFromPrimaryKey());

      assertEquals(0, actorVertexType.getOutEdgesType().size());
      assertEquals(1, actorVertexType.getInEdgesType().size());
      assertEquals(featuresEdgeType, actorVertexType.getInEdgesType().get(0));

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("category"));
      assertEquals("category", filmVertexType.getPropertyByName("category").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("category").getOriginalType());
      assertEquals(3, filmVertexType.getPropertyByName("category").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("category").isFromPrimaryKey());

      assertEquals(1, filmVertexType.getOutEdgesType().size());
      assertEquals(0, filmVertexType.getInEdgesType().size());
      assertEquals(featuresEdgeType, filmVertexType.getOutEdgesType().get(0));

      // edges check
      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(featuresEdgeType);

      assertEquals("Features", featuresEdgeType.getName());
      assertEquals(2, featuresEdgeType.getProperties().size());
      assertEquals("Actor", featuresEdgeType.getInVertexType().getName());
      assertEquals(1, featuresEdgeType.getNumberRelationshipsRepresented());

      assertEquals(2, featuresEdgeType.getAllProperties().size());
      OModelProperty paymentProperty = featuresEdgeType.getPropertyByName("payment");
      assertNotNull(paymentProperty);
      assertEquals("payment", paymentProperty.getName());
      assertEquals(1, paymentProperty.getOrdinalPosition());
      assertEquals(false, paymentProperty.isFromPrimaryKey());
      assertEquals("INTEGER", paymentProperty.getOriginalType());
      assertNull(paymentProperty.isMandatory());
      assertNull(paymentProperty.isReadOnly());
      assertNull(paymentProperty.isNotNull());

      yearProperty = featuresEdgeType.getPropertyByName("year");
      assertNotNull(yearProperty);
      assertEquals("year", yearProperty.getName());
      assertEquals(2, yearProperty.getOrdinalPosition());
      assertEquals(false, yearProperty.isFromPrimaryKey());
      assertEquals("DATE", yearProperty.getOrientdbType());
      assertEquals(false, yearProperty.isMandatory());
      assertEquals(false, yearProperty.isReadOnly());
      assertEquals(false, yearProperty.isNotNull());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2EVClassMappers().size());
      assertEquals(3, mapper.getEntity2EVClassMappers().size());

      assertEquals(1, mapper.getEVClassMappersByVertex(actorVertexType).size());
      actorClassMapper = mapper.getEVClassMappersByVertex(actorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(actorEntity).size());
      assertEquals(actorClassMapper, mapper.getEVClassMappersByEntity(actorEntity).get(0));
      assertEquals(actorClassMapper.getEntity(), actorEntity);
      assertEquals(actorClassMapper.getVertexType(), actorVertexType);

      assertEquals(3, actorClassMapper.getAttribute2property().size());
      assertEquals(3, actorClassMapper.getProperty2attribute().size());
      assertEquals("id", actorClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", actorClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", actorClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", actorClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", actorClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", actorClassMapper.getProperty2attribute().get("lastName"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmVertexType).size());
      filmClassMapper = mapper.getEVClassMappersByVertex(filmVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmEntity).size());
      assertEquals(filmClassMapper, mapper.getEVClassMappersByEntity(filmEntity).get(0));
      assertEquals(filmClassMapper.getEntity(), filmEntity);
      assertEquals(filmClassMapper.getVertexType(), filmVertexType);

      assertEquals(3, filmClassMapper.getAttribute2property().size());
      assertEquals(3, filmClassMapper.getProperty2attribute().size());
      assertEquals("id", filmClassMapper.getAttribute2property().get("ID"));
      assertEquals("title", filmClassMapper.getAttribute2property().get("TITLE"));
      assertEquals("category", filmClassMapper.getAttribute2property().get("CATEGORY"));
      assertEquals("ID", filmClassMapper.getProperty2attribute().get("id"));
      assertEquals("TITLE", filmClassMapper.getProperty2attribute().get("title"));
      assertEquals("CATEGORY", filmClassMapper.getProperty2attribute().get("category"));

      assertEquals(1, mapper.getEVClassMappersByVertex(filmActorVertexType).size());
      actorFilmClassMapper = mapper.getEVClassMappersByVertex(filmActorVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(filmActorEntity).size());
      assertEquals(actorFilmClassMapper, mapper.getEVClassMappersByEntity(filmActorEntity).get(0));
      assertEquals(actorFilmClassMapper.getEntity(), filmActorEntity);
      assertEquals(actorFilmClassMapper.getVertexType(), filmActorVertexType);

      assertEquals(3, actorFilmClassMapper.getAttribute2property().size());
      assertEquals(3, actorFilmClassMapper.getProperty2attribute().size());
      assertEquals("actorId", actorFilmClassMapper.getAttribute2property().get("ACTOR_ID"));
      assertEquals("filmId", actorFilmClassMapper.getAttribute2property().get("FILM_ID"));
      assertEquals("payment", actorFilmClassMapper.getAttribute2property().get("PAYMENT"));
      assertEquals("ACTOR_ID", actorFilmClassMapper.getProperty2attribute().get("actorId"));
      assertEquals("FILM_ID", actorFilmClassMapper.getProperty2attribute().get("filmId"));
      assertEquals("PAYMENT", actorFilmClassMapper.getProperty2attribute().get("payment"));

      // Relationships-Edges Mapping

      itRelationships = filmActorEntity.getOutCanonicalRelationships().iterator();
      performsLeftRelationship = itRelationships.next();
      performsRightRelationship = itRelationships.next();
      assertFalse(itRelationships.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(featuresLeftEdgeType, mapper.getRelationship2edgeType().get(performsLeftRelationship));
      assertEquals(featuresRightEdgeType, mapper.getRelationship2edgeType().get(performsRightRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(featuresLeftEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(featuresLeftEdgeType).contains(performsLeftRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(featuresRightEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(featuresRightEdgeType).contains(performsRightRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(1, mapper.getJoinVertex2aggregatorEdges().size());
      assertTrue(mapper.getJoinVertex2aggregatorEdges().containsKey(filmActorVertexType));
      assertEquals(featuresEdgeType, mapper.getJoinVertex2aggregatorEdges().get(filmActorVertexType).getEdgeType());
      assertEquals("Film", mapper.getJoinVertex2aggregatorEdges().get(filmActorVertexType).getOutVertexClassName());
      assertEquals("Actor", mapper.getJoinVertex2aggregatorEdges().get(filmActorVertexType).getInVertexClassName());

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
