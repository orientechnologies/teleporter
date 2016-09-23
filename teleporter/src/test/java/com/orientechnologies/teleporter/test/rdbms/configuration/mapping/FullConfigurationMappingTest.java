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
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OClassMapper;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.ORelationship;
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
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class FullConfigurationMappingTest {

    private OER2GraphMapper mapper;
    private OTeleporterContext context;
    private final String config = "src/test/resources/configuration-mapping/full-configuration-mapping.json";
    private ODBQueryEngine dbQueryEngine;
    private String driver = "org.hsqldb.jdbc.JDBCDriver";
    private String jurl = "jdbc:hsqldb:mem:mydb";
    private String username = "SA";
    private String password = "";

    @Before
    public void init() {
        this.context = new OTeleporterContext();
        this.dbQueryEngine = new ODBQueryEngine(this.driver, this.jurl, this.username, this.password, this.context);
        this.context.setDbQueryEngine(this.dbQueryEngine);
        this.context.setOutputManager(new OOutputStreamManager(0));
        this.context.setNameResolver(new OJavaConventionNameResolver());
        this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
        context.setOutputManager(new OOutputStreamManager(0));
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

            this.mapper = new OER2GraphMapper(this.driver, this.jurl, this.username, this.password, null, null, config);
            this.mapper.buildSourceDatabaseSchema(this.context);
            this.mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
            this.mapper.applyImportConfiguration(this.context);


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
            assertEquals(1, mapper.getDataBaseSchema().getRelationships().size());
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
            assertEquals(1, mapper.getDataBaseSchema().getRelationships().size());
            assertEquals(0, vatProfileEntity.getOutRelationships().size());
            assertEquals(1, personEntity.getOutRelationships().size());
            assertEquals(0, departmentEntity.getOutRelationships().size());
            assertEquals(0, vatProfileEntity.getInRelationships().size());
            assertEquals(0, personEntity.getInRelationships().size());
            assertEquals(1, departmentEntity.getInRelationships().size());
            assertEquals(0, vatProfileEntity.getForeignKeys().size());
            assertEquals(1, personEntity.getForeignKeys().size());
            assertEquals(0, departmentEntity.getForeignKeys().size());

            Iterator<ORelationship> it = personEntity.getOutRelationships().iterator();
            ORelationship currentRelationship = it.next();
            assertEquals("DEPARTMENT", currentRelationship.getParentEntity().getName());
            assertEquals("PERSON", currentRelationship.getForeignEntity().getName());
            assertEquals(departmentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
            assertEquals(personEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

            Iterator<ORelationship> it2 = departmentEntity.getInRelationships().iterator();
            ORelationship currentRelationship2 = it2.next();
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
            assertEquals("string", personVertexType.getPropertyByName("extKey1").getOrientdbType());
            assertEquals(1, personVertexType.getPropertyByName("extKey1").getOrdinalPosition());
            assertEquals(true, personVertexType.getPropertyByName("extKey1").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("extKey1").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("firstName"));
            assertEquals("firstName", personVertexType.getPropertyByName("firstName").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("firstName").getOriginalType());
            assertEquals("string", personVertexType.getPropertyByName("firstName").getOrientdbType());
            assertEquals(2, personVertexType.getPropertyByName("firstName").getOrdinalPosition());
            assertEquals(false, personVertexType.getPropertyByName("firstName").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("firstName").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("lastName"));
            assertEquals("lastName", personVertexType.getPropertyByName("lastName").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("lastName").getOriginalType());
            assertEquals("string", personVertexType.getPropertyByName("lastName").getOrientdbType());
            assertEquals(3, personVertexType.getPropertyByName("lastName").getOrdinalPosition());
            assertEquals(false, personVertexType.getPropertyByName("lastName").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("lastName").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("depId"));
            assertEquals("depId", personVertexType.getPropertyByName("depId").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("depId").getOriginalType());
            assertEquals("string", personVertexType.getPropertyByName("depId").getOrientdbType());
            assertEquals(4, personVertexType.getPropertyByName("depId").getOrdinalPosition());
            assertEquals(false, personVertexType.getPropertyByName("depId").isFromPrimaryKey());
            assertEquals(false, personVertexType.getPropertyByName("depId").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("extKey2"));
            assertEquals("extKey2", personVertexType.getPropertyByName("extKey2").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("extKey2").getOriginalType());
            assertEquals("string", personVertexType.getPropertyByName("extKey2").getOrientdbType());
            assertEquals(5, personVertexType.getPropertyByName("extKey2").getOrdinalPosition());
            assertEquals(true, personVertexType.getPropertyByName("extKey2").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("extKey2").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("VAT"));
            assertEquals("VAT", personVertexType.getPropertyByName("VAT").getName());
            assertEquals("VARCHAR", personVertexType.getPropertyByName("VAT").getOriginalType());
            assertEquals("string", personVertexType.getPropertyByName("VAT").getOrientdbType());
            assertEquals(6, personVertexType.getPropertyByName("VAT").getOrdinalPosition());
            assertEquals(false, personVertexType.getPropertyByName("VAT").isFromPrimaryKey());
            assertEquals(true, personVertexType.getPropertyByName("VAT").isIncludedInMigration());

            assertNotNull(personVertexType.getPropertyByName("updatedOn"));
            assertEquals("updatedOn", personVertexType.getPropertyByName("updatedOn").getName());
            assertEquals("DATE", personVertexType.getPropertyByName("updatedOn").getOriginalType());
            assertEquals("date", personVertexType.getPropertyByName("updatedOn").getOrientdbType());
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
            assertEquals("string", departmentVertexType.getPropertyByName("id").getOrientdbType());
            assertEquals(1, departmentVertexType.getPropertyByName("id").getOrdinalPosition());
            assertEquals(true, departmentVertexType.getPropertyByName("id").isFromPrimaryKey());
            assertEquals(true, departmentVertexType.getPropertyByName("id").isIncludedInMigration());

            assertNotNull(departmentVertexType.getPropertyByName("departmentName"));
            assertEquals("departmentName", departmentVertexType.getPropertyByName("departmentName").getName());
            assertEquals("VARCHAR", departmentVertexType.getPropertyByName("departmentName").getOriginalType());
            assertEquals("string", departmentVertexType.getPropertyByName("departmentName").getOrientdbType());
            assertEquals(2, departmentVertexType.getPropertyByName("departmentName").getOrdinalPosition());
            assertEquals(false, departmentVertexType.getPropertyByName("departmentName").isFromPrimaryKey());
            assertEquals(true, departmentVertexType.getPropertyByName("departmentName").isIncludedInMigration());

            assertNotNull(departmentVertexType.getPropertyByName("location"));
            assertEquals("location", departmentVertexType.getPropertyByName("location").getName());
            assertEquals("VARCHAR", departmentVertexType.getPropertyByName("location").getOriginalType());
            assertEquals("string", departmentVertexType.getPropertyByName("location").getOrientdbType());
            assertEquals(3, departmentVertexType.getPropertyByName("location").getOrdinalPosition());
            assertEquals(false, departmentVertexType.getPropertyByName("location").isFromPrimaryKey());
            assertEquals(true, departmentVertexType.getPropertyByName("location").isIncludedInMigration());

            assertNotNull(departmentVertexType.getPropertyByName("updatedOn"));
            assertEquals("updatedOn", departmentVertexType.getPropertyByName("updatedOn").getName());
            assertEquals("DATE", departmentVertexType.getPropertyByName("updatedOn").getOriginalType());
            assertEquals("date", departmentVertexType.getPropertyByName("updatedOn").getOrientdbType());
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
            assertEquals("date", sinceProperty.getOriginalType());
            assertEquals(true, sinceProperty.isMandatory());
            assertEquals(false, sinceProperty.isReadOnly());
            assertEquals(false, sinceProperty.isNotNull());


            /*
             * Rules check
             */

            // Classes Mapping

            assertEquals(2, mapper.getVertexType2classMappers().size());
            assertEquals(3, mapper.getEntity2classMappers().size());

            assertEquals(2, mapper.getClassMappersByVertex(personVertexType).size());
            OClassMapper personClassMapper = mapper.getClassMappersByVertex(personVertexType).get(0);
            assertEquals(1, mapper.getClassMappersByEntity(personEntity).size());
            assertEquals(personClassMapper, mapper.getClassMappersByEntity(personEntity).get(0));
            assertEquals(personClassMapper.getEntity(), personEntity);
            assertEquals(personClassMapper.getVertexType(), personVertexType);

            assertEquals(4, personClassMapper.attribute2property.size());
            assertEquals(3, personClassMapper.property2attribute.size());
            assertEquals("extKey1", personClassMapper.attribute2property.get("ID"));
            assertEquals("firstName", personClassMapper.attribute2property.get("NAME"));
            assertEquals("lastName", personClassMapper.attribute2property.get("SURNAME"));
            assertNull(personClassMapper.attribute2property.get("DEP_ID"));
            assertEquals("ID", personClassMapper.property2attribute.get("extKey1"));
            assertEquals("NAME", personClassMapper.property2attribute.get("firstName"));
            assertEquals("SURNAME", personClassMapper.property2attribute.get("lastName"));

            OClassMapper vatProfileClassMapper = mapper.getClassMappersByVertex(personVertexType).get(1);
            assertEquals(1, mapper.getClassMappersByEntity(vatProfileEntity).size());
            assertEquals(vatProfileClassMapper, mapper.getClassMappersByEntity(vatProfileEntity).get(0));
            assertEquals(vatProfileClassMapper.getEntity(), vatProfileEntity);
            assertEquals(vatProfileClassMapper.getVertexType(), personVertexType);

            assertEquals(3, vatProfileClassMapper.attribute2property.size());
            assertEquals(2, vatProfileClassMapper.property2attribute.size());
            assertEquals("extKey2", vatProfileClassMapper.attribute2property.get("ID"));
            assertEquals("VAT", vatProfileClassMapper.attribute2property.get("VAT"));
            assertNull(vatProfileClassMapper.attribute2property.get("UPDATED_ON"));
            assertEquals("ID", vatProfileClassMapper.property2attribute.get("extKey2"));
            assertEquals("VAT", vatProfileClassMapper.property2attribute.get("VAT"));

            assertEquals(1, mapper.getClassMappersByVertex(departmentVertexType).size());
            OClassMapper departmentClassMapper = mapper.getClassMappersByVertex(departmentVertexType).get(0);
            assertEquals(1, mapper.getClassMappersByEntity(departmentEntity).size());
            assertEquals(departmentClassMapper, mapper.getClassMappersByEntity(departmentEntity).get(0));
            assertEquals(departmentClassMapper.getEntity(), departmentEntity);
            assertEquals(departmentClassMapper.getVertexType(), departmentVertexType);

            assertEquals(4, departmentClassMapper.attribute2property.size());
            assertEquals(3, departmentClassMapper.property2attribute.size());
            assertEquals("id", departmentClassMapper.attribute2property.get("ID"));
            assertEquals("departmentName", departmentClassMapper.attribute2property.get("NAME"));
            assertEquals("location", departmentClassMapper.attribute2property.get("LOCATION"));
            assertNull(departmentClassMapper.attribute2property.get("UPDATED_ON"));
            assertEquals("ID", departmentClassMapper.property2attribute.get("id"));
            assertEquals("NAME", departmentClassMapper.property2attribute.get("departmentName"));
            assertEquals("LOCATION", departmentClassMapper.property2attribute.get("location"));

            // Relationships-Edges Mapping

            Iterator<ORelationship> itRelationships = personEntity.getOutRelationships().iterator();
            ORelationship worksAtRelationship = itRelationships.next();
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
