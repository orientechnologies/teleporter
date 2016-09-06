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

package com.orientechnologies.teleporter.test.rdbms.configuration.orientWriter;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.util.OFileManager;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class OrientDBSchemaWritingWithFullConfigTest {

    private OER2GraphMapper mapper;
    private OTeleporterContext context;
    private OGraphModelWriter modelWriter;
    private String             outOrientGraphUri;
    private final String config = "src/test/resources/configuration-mapping/full-configuration-mapping.json";

    @Before
    public void init() {
        this.context = new OTeleporterContext();
        this.context.setOutputManager(new OOutputStreamManager(0));
        this.context.setNameResolver(new OJavaConventionNameResolver());
        this.context.setQueryQuoteType("\"");
        this.modelWriter = new OGraphModelWriter();
        this.outOrientGraphUri = "memory:testOrientDB";
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
    @Ignore
    public void test1() {

        Connection connection = null;
        Statement st = null;
        OrientGraphNoTx orientGraph = null;

        try {

            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

            String personTableBuilding = "create memory table PERSON (ID varchar(256) not null,"+
                    " NAME varchar(256) not null, SURNAME varchar(256) not null, DEP_ID varchar(256) not null, primary key (ID))";
            st = connection.createStatement();
            st.execute(personTableBuilding);

            String vatProfileTableBuilding = "create memory table VAT_PROFILE (ID varchar(256),"+
                    " VAT varchar(256) not null, UPDATED_ON date not null, primary key (ID))";
            st.execute(vatProfileTableBuilding);

            String departmentTableBuilding = "create memory table DEPARTMENT (ID  varchar(256),"+
                    " NAME varchar(256) not null, LOCATION varchar(256) not null, UPDATED_ON date not null, primary key (ID))";
            st.execute(departmentTableBuilding);

            ODocument config = OFileManager.buildJsonFromFile(this.config);

            this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null, config);
            mapper.buildSourceDatabaseSchema(this.context);
            mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
            mapper.applyImportConfiguration(this.context);
            modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);


      /*
       *  Testing context information
       */

            assertEquals(2, context.getStatistics().totalNumberOfVertexTypes);
            assertEquals(2, context.getStatistics().wroteVertexType);
            assertEquals(2, context.getStatistics().totalNumberOfEdgeTypes);
            assertEquals(2, context.getStatistics().wroteEdgeType);
            assertEquals(2, context.getStatistics().totalNumberOfIndices);
            assertEquals(2, context.getStatistics().wroteIndexes);

      /*
       *  Testing built OrientDB schema     TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
            assertEquals(false, employeeVertexType.getProperty("firstName").isMandatory());
            assertEquals(false, employeeVertexType.getProperty("firstName").isReadonly());
            assertEquals(false, employeeVertexType.getProperty("firstName").isNotNull());

            assertNotNull(employeeVertexType.getProperty("lastName"));
            assertEquals("lastName", employeeVertexType.getProperty("lastName").getName());
            assertEquals(OType.STRING, employeeVertexType.getProperty("lastName").getType());
            assertEquals(false, employeeVertexType.getProperty("lastName").isMandatory());
            assertEquals(false, employeeVertexType.getProperty("lastName").isReadonly());
            assertEquals(false, employeeVertexType.getProperty("lastName").isNotNull());

            assertNotNull(employeeVertexType.getProperty("project"));
            assertEquals("project", employeeVertexType.getProperty("project").getName());
            assertEquals(OType.STRING, employeeVertexType.getProperty("project").getType());
            assertEquals(false, employeeVertexType.getProperty("project").isMandatory());
            assertEquals(false, employeeVertexType.getProperty("project").isReadonly());
            assertEquals(false, employeeVertexType.getProperty("project").isNotNull());

            assertNotNull(projectVertexType.getProperty("id"));
            assertEquals("id", projectVertexType.getProperty("id").getName());
            assertEquals(OType.STRING, projectVertexType.getProperty("id").getType());
            assertEquals(false, projectVertexType.getProperty("id").isMandatory());
            assertEquals(false, projectVertexType.getProperty("id").isReadonly());
            assertEquals(false, projectVertexType.getProperty("id").isNotNull());

            assertNotNull(projectVertexType.getProperty("title"));
            assertEquals("title", projectVertexType.getProperty("title").getName());
            assertEquals(OType.STRING, projectVertexType.getProperty("title").getType());
            assertEquals(false, projectVertexType.getProperty("title").isMandatory());
            assertEquals(false, projectVertexType.getProperty("title").isReadonly());
            assertEquals(false, projectVertexType.getProperty("title").isNotNull());

            assertNotNull(projectVertexType.getProperty("projectManager"));
            assertEquals("projectManager", projectVertexType.getProperty("projectManager").getName());
            assertEquals(OType.STRING, projectVertexType.getProperty("projectManager").getType());
            assertEquals(false, projectVertexType.getProperty("projectManager").isMandatory());
            assertEquals(false, projectVertexType.getProperty("projectManager").isReadonly());
            assertEquals(false, projectVertexType.getProperty("projectManager").isNotNull());

            // edges check
            assertNotNull(worksAtProjectEdgeType);
            assertNotNull(hasManagerEdgeType);

            assertEquals("WorksAtProject", worksAtProjectEdgeType.getName());
            assertEquals(1, worksAtProjectEdgeType.propertiesMap().size());

            assertEquals("updatedOn", worksAtProjectEdgeType.getProperty("updatedOn").getName());
            assertEquals(OType.DATE, worksAtProjectEdgeType.getProperty("updatedOn").getType());
            assertEquals(true, worksAtProjectEdgeType.getProperty("updatedOn").isMandatory());
            assertEquals(false, worksAtProjectEdgeType.getProperty("updatedOn").isReadonly());
            assertEquals(false, worksAtProjectEdgeType.getProperty("updatedOn").isNotNull());

            assertEquals("HasManager", hasManagerEdgeType.getName());
            assertEquals(1, hasManagerEdgeType.propertiesMap().size());

            assertEquals("updatedOn", hasManagerEdgeType.getProperty("updatedOn").getName());
            assertEquals(OType.DATE, hasManagerEdgeType.getProperty("updatedOn").getType());
            assertEquals(false, hasManagerEdgeType.getProperty("updatedOn").isMandatory());
            assertEquals(false, hasManagerEdgeType.getProperty("updatedOn").isReadonly());
            assertEquals(false, hasManagerEdgeType.getProperty("updatedOn").isNotNull());

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
            if(orientGraph != null) {
                orientGraph.drop();
                orientGraph.shutdown();
            }
        }

    }

}
