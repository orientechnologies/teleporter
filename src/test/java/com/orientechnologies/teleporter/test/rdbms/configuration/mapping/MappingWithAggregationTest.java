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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
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
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

/**
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public class MappingWithAggregationTest {

    private OER2GraphMapper mapper;
    private OTeleporterContext context;
    private final String config = "src/test/resources/configuration-mapping/aggregation-from2tables-mapping.json";
    private ODBQueryEngine dbQueryEngine;
    private String driver = "org.hsqldb.jdbc.JDBCDriver";
    private String jurl = "jdbc:hsqldb:mem:mydb";
    private String username = "SA";
    private String password = "";
    private OSourceDatabaseInfo sourceDBInfo;

    @Before
    public void init() {
        this.context = OTeleporterContext.newInstance();
        this.dbQueryEngine = new ODBQueryEngine(this.driver);
        this.context.setDbQueryEngine(this.dbQueryEngine);
        this.context.setOutputManager(new OOutputStreamManager(0));
        this.context.setNameResolver(new OJavaConventionNameResolver());
        this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
        context.setOutputManager(new OOutputStreamManager(0));
        this.sourceDBInfo = new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
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

        try {

            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String personTableBuilding = "create memory table PERSON (ID varchar(256) not null,"+
                    " NAME varchar(256) not null, SURNAME varchar(256) not null, DEP_ID varchar(256) not null, primary key (ID))";
            st = connection.createStatement();
            st.execute(personTableBuilding);

            String vatProfileTableBuilding = "create memory table VAT_PROFILE (ID varchar(256),"+
                    " VAT varchar(256) not null, UPDATED_ON date not null, primary key (ID))";
            st.execute(vatProfileTableBuilding);

            String departmentTableBuilding = "create memory table DEPARTMENT (ID varchar(256),"+
                    " NAME varchar(256) not null, LOCATION varchar(256) not null, UPDATED_ON date not null, primary key (ID))";
            st.execute(departmentTableBuilding);

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
            assertEquals(1, context.getStatistics().totalNumberOfRelationships);
            assertEquals(1, context.getStatistics().builtRelationships);

            assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
            assertEquals(2, context.getStatistics().builtModelVertexTypes);
            assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
            assertEquals(1, context.getStatistics().builtModelEdgeTypes);

            /*
             *  Testing built source db schema
             */

            OEntity personEntity = mapper.getDataBaseSchema().getEntityByName("PERSON");
            OEntity vatProfileEntity = mapper.getDataBaseSchema().getEntityByName("VAT_PROFILE");
            OEntity departmentEntity = mapper.getDataBaseSchema().getEntityByName("DEPARTMENT");

            // entities check
            assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
            assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
            assertNotNull(personEntity);
            assertNotNull(vatProfileEntity);
            assertNotNull(departmentEntity);

            // attributes check
            assertEquals(4, personEntity.getAttributes().size());

            assertNotNull(personEntity.getAttributeByName("ID"));
            assertEquals("ID", personEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", personEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, personEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("PERSON", personEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(personEntity.getAttributeByName("NAME"));
            assertEquals("NAME", personEntity.getAttributeByName("NAME").getName());
            assertEquals("VARCHAR", personEntity.getAttributeByName("NAME").getDataType());
            assertEquals(2, personEntity.getAttributeByName("NAME").getOrdinalPosition());
            assertEquals("PERSON", personEntity.getAttributeByName("NAME").getBelongingEntity().getName());

            assertNotNull(personEntity.getAttributeByName("SURNAME"));
            assertEquals("SURNAME", personEntity.getAttributeByName("SURNAME").getName());
            assertEquals("VARCHAR", personEntity.getAttributeByName("SURNAME").getDataType());
            assertEquals(3, personEntity.getAttributeByName("SURNAME").getOrdinalPosition());
            assertEquals("PERSON", personEntity.getAttributeByName("SURNAME").getBelongingEntity().getName());

            assertNotNull(personEntity.getAttributeByName("DEP_ID"));
            assertEquals("DEP_ID", personEntity.getAttributeByName("DEP_ID").getName());
            assertEquals("VARCHAR", personEntity.getAttributeByName("DEP_ID").getDataType());
            assertEquals(4, personEntity.getAttributeByName("DEP_ID").getOrdinalPosition());
            assertEquals("PERSON", personEntity.getAttributeByName("DEP_ID").getBelongingEntity().getName());

            assertEquals(3, vatProfileEntity.getAttributes().size());

            assertNotNull(vatProfileEntity.getAttributeByName("ID"));
            assertEquals("ID", vatProfileEntity.getAttributeByName("ID").getName());
            assertEquals("VARCHAR", vatProfileEntity.getAttributeByName("ID").getDataType());
            assertEquals(1, vatProfileEntity.getAttributeByName("ID").getOrdinalPosition());
            assertEquals("VAT_PROFILE", vatProfileEntity.getAttributeByName("ID").getBelongingEntity().getName());

            assertNotNull(vatProfileEntity.getAttributeByName("VAT"));
            assertEquals("VAT", vatProfileEntity.getAttributeByName("VAT").getName());
            assertEquals("VARCHAR", vatProfileEntity.getAttributeByName("VAT").getDataType());
            assertEquals(2, vatProfileEntity.getAttributeByName("VAT").getOrdinalPosition());
            assertEquals("VAT_PROFILE", vatProfileEntity.getAttributeByName("VAT").getBelongingEntity().getName());

            assertNotNull(vatProfileEntity.getAttributeByName("UPDATED_ON"));
            assertEquals("UPDATED_ON", vatProfileEntity.getAttributeByName("UPDATED_ON").getName());
            assertEquals("DATE", vatProfileEntity.getAttributeByName("UPDATED_ON").getDataType());
            assertEquals(3, vatProfileEntity.getAttributeByName("UPDATED_ON").getOrdinalPosition());
            assertEquals("VAT_PROFILE", vatProfileEntity.getAttributeByName("UPDATED_ON").getBelongingEntity().getName());

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


            // relationship, primary and foreign key check
            assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
            assertEquals(0, vatProfileEntity.getOutCanonicalRelationships().size());
            assertEquals(1, personEntity.getOutCanonicalRelationships().size());
            assertEquals(0, departmentEntity.getOutCanonicalRelationships().size());
            assertEquals(0, vatProfileEntity.getInCanonicalRelationships().size());
            assertEquals(0, personEntity.getInCanonicalRelationships().size());
            assertEquals(1, departmentEntity.getInCanonicalRelationships().size());
            assertEquals(0, vatProfileEntity.getForeignKeys().size());
            assertEquals(1, personEntity.getForeignKeys().size());
            assertEquals(0, departmentEntity.getForeignKeys().size());

            Iterator<OCanonicalRelationship> it = personEntity.getOutCanonicalRelationships().iterator();
            OCanonicalRelationship currentRelationship = it.next();
            assertEquals("DEPARTMENT", currentRelationship.getParentEntity().getName());
            assertEquals("PERSON", currentRelationship.getForeignEntity().getName());
            assertEquals(departmentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
            assertEquals(personEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

            Iterator<OCanonicalRelationship> it2 = departmentEntity.getInCanonicalRelationships().iterator();
            OCanonicalRelationship currentRelationship2 = it2.next();
            assertEquals(currentRelationship, currentRelationship2);

            assertEquals("DEP_ID", personEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
            assertEquals("ID", departmentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

            assertFalse(it.hasNext());
            assertFalse(it2.hasNext());


            /*
             *  Testing built graph model
             */

            OVertexType personVertexType = mapper.getGraphModel().getVertexTypeByName("Person");
            OVertexType departmentVertexType = mapper.getGraphModel().getVertexTypeByName("Department");
            OEdgeType worksAtEdgeType = mapper.getGraphModel().getEdgeTypeByName("WorksAt");


            // vertices check
            assertEquals(2, mapper.getGraphModel().getVerticesType().size());
            assertNotNull(personVertexType);
            assertNotNull(departmentVertexType);

            // properties check
            assertEquals(7, personVertexType.getProperties().size());

            assertNotNull(personVertexType.getPropertyByName("extKey1"));
            assertEquals("extKey1", personVertexType.getPropertyByName("extKey1").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("extKey1").getOriginalType());
            assertEquals("STRING", personVertexType.getPropertyByName("extKey1").getOrientdbType());
            assertEquals(1, personVertexType.getPropertyByName("extKey1").getOrdinalPosition());
            assertEquals(true, personVertexType.getPropertyByName("extKey1").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("extKey1").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("firstName"));
            assertEquals("firstName", personVertexType.getPropertyByName("firstName").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("firstName").getOriginalType());
            assertEquals("STRING", personVertexType.getPropertyByName("firstName").getOrientdbType());
            assertEquals(2, personVertexType.getPropertyByName("firstName").getOrdinalPosition());
            assertEquals(false, personVertexType.getPropertyByName("firstName").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("firstName").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("lastName"));
            assertEquals("lastName", personVertexType.getPropertyByName("lastName").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("lastName").getOriginalType());
            assertEquals("STRING", personVertexType.getPropertyByName("lastName").getOrientdbType());
            assertEquals(3, personVertexType.getPropertyByName("lastName").getOrdinalPosition());
            assertEquals(false, personVertexType.getPropertyByName("lastName").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("lastName").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("depId"));
            assertEquals("depId", personVertexType.getPropertyByName("depId").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("depId").getOriginalType());
            assertEquals("STRING", personVertexType.getPropertyByName("depId").getOrientdbType());
            assertEquals(4, personVertexType.getPropertyByName("depId").getOrdinalPosition());
            assertEquals(false, personVertexType.getPropertyByName("depId").isFromPrimaryKey());
            assertEquals(false, personVertexType.getPropertyByName("depId").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("extKey2"));
            assertEquals("extKey2", personVertexType.getPropertyByName("extKey2").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("extKey2").getOriginalType());
            assertEquals("STRING", personVertexType.getPropertyByName("extKey2").getOrientdbType());
            assertEquals(5, personVertexType.getPropertyByName("extKey2").getOrdinalPosition());
            assertEquals(true, personVertexType.getPropertyByName("extKey2").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("extKey2").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("VAT"));
            assertEquals("VAT", personVertexType.getPropertyByName("VAT").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("VAT").getOriginalType());
            assertEquals("STRING", personVertexType.getPropertyByName("VAT").getOrientdbType());
            assertEquals(6, personVertexType.getPropertyByName("VAT").getOrdinalPosition());
            assertEquals(false, personVertexType.getPropertyByName("VAT").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("VAT").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("updatedOn"));
            assertEquals("updatedOn", personVertexType.getPropertyByName("updatedOn").getName());
            assertEquals("DATE", personVertexType.getPropertyByName("updatedOn").getOriginalType());
            assertEquals("DATE", personVertexType.getPropertyByName("updatedOn").getOrientdbType());
            assertEquals(7, personVertexType.getPropertyByName("updatedOn").getOrdinalPosition());
            assertEquals(false, personVertexType.getPropertyByName("updatedOn").isFromPrimaryKey());
            assertEquals(false, personVertexType.getPropertyByName("updatedOn").isIncludedInMigration());

            assertEquals(1, personVertexType.getOutEdgesType().size());
            assertEquals(worksAtEdgeType, personVertexType.getOutEdgesType().get(0));
            assertEquals(0, personVertexType.getInEdgesType().size());

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

            // edges check
            assertEquals(1, mapper.getGraphModel().getEdgesType().size());
            assertNotNull(worksAtEdgeType);

            assertEquals("WorksAt", worksAtEdgeType.getName());
            assertEquals(1, worksAtEdgeType.getProperties().size());
            assertEquals("Department", worksAtEdgeType.getInVertexType().getName());
            assertEquals(1, worksAtEdgeType.getNumberRelationshipsRepresented());

            assertEquals(1, worksAtEdgeType.getAllProperties().size());
            OModelProperty sinceProperty = worksAtEdgeType.getPropertyByName("since");
            assertNotNull(sinceProperty);
            assertEquals("since", sinceProperty.getName());
            assertEquals(1, sinceProperty.getOrdinalPosition());
            assertEquals(false, sinceProperty.isFromPrimaryKey());
            assertEquals("DATE", sinceProperty.getOrientdbType());
            assertEquals(true, sinceProperty.isMandatory());
            assertEquals(false, sinceProperty.isReadOnly());
            assertEquals(false, sinceProperty.isNotNull());


            /*
             * Rules check
             */

            // Classes Mapping

            assertEquals(2, mapper.getVertexType2EVClassMappers().size());
            assertEquals(3, mapper.getEntity2EVClassMappers().size());

            assertEquals(2, mapper.getEVClassMappersByVertex(personVertexType).size());
            OEVClassMapper personClassMapper = mapper.getEVClassMappersByVertex(personVertexType).get(0);
            assertEquals(1, mapper.getEVClassMappersByEntity(personEntity).size());
            assertEquals(personClassMapper, mapper.getEVClassMappersByEntity(personEntity).get(0));
            assertEquals(personClassMapper.getEntity(), personEntity);
            assertEquals(personClassMapper.getVertexType(), personVertexType);

            assertEquals(4, personClassMapper.getAttribute2property().size());
            assertEquals(4, personClassMapper.getProperty2attribute().size());
            assertEquals("extKey1", personClassMapper.getAttribute2property().get("ID"));
            assertEquals("firstName", personClassMapper.getAttribute2property().get("NAME"));
            assertEquals("lastName", personClassMapper.getAttribute2property().get("SURNAME"));
            assertEquals("depId", personClassMapper.getAttribute2property().get("DEP_ID"));
            assertEquals("ID", personClassMapper.getProperty2attribute().get("extKey1"));
            assertEquals("NAME", personClassMapper.getProperty2attribute().get("firstName"));
            assertEquals("SURNAME", personClassMapper.getProperty2attribute().get("lastName"));
            assertEquals("DEP_ID", personClassMapper.getProperty2attribute().get("depId"));

            OEVClassMapper vatProfileClassMapper = mapper.getEVClassMappersByVertex(personVertexType).get(1);
            assertEquals(1, mapper.getEVClassMappersByEntity(vatProfileEntity).size());
            assertEquals(vatProfileClassMapper, mapper.getEVClassMappersByEntity(vatProfileEntity).get(0));
            assertEquals(vatProfileClassMapper.getEntity(), vatProfileEntity);
            assertEquals(vatProfileClassMapper.getVertexType(), personVertexType);

            assertEquals(3, vatProfileClassMapper.getAttribute2property().size());
            assertEquals(3, vatProfileClassMapper.getProperty2attribute().size());
            assertEquals("extKey2", vatProfileClassMapper.getAttribute2property().get("ID"));
            assertEquals("VAT", vatProfileClassMapper.getAttribute2property().get("VAT"));
            assertEquals("updatedOn", vatProfileClassMapper.getAttribute2property().get("UPDATED_ON"));
            assertEquals("ID", vatProfileClassMapper.getProperty2attribute().get("extKey2"));
            assertEquals("VAT", vatProfileClassMapper.getProperty2attribute().get("VAT"));
            assertEquals("UPDATED_ON", vatProfileClassMapper.getProperty2attribute().get("updatedOn"));

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

            // Relationships-Edges Mapping

            Iterator<OCanonicalRelationship> itRelationships = personEntity.getOutCanonicalRelationships().iterator();
            OCanonicalRelationship worksAtRelationship = itRelationships.next();
            assertFalse(itRelationships.hasNext());

            assertEquals(1, mapper.getRelationship2edgeType().size());
            assertEquals(worksAtEdgeType, mapper.getRelationship2edgeType().get(worksAtRelationship));

            assertEquals(1, mapper.getEdgeType2relationships().size());
            assertEquals(1, mapper.getEdgeType2relationships().get(worksAtEdgeType).size());
            assertTrue(mapper.getEdgeType2relationships().get(worksAtEdgeType).contains(worksAtRelationship));

            // JoinVertexes-AggregatorEdges Mapping

            assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());


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
