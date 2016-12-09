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

package com.orientechnologies.teleporter.test.rdbms.configuration.importing;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.orientechnologies.teleporter.util.OFileManager;
import com.orientechnologies.teleporter.util.OMigrationConfigManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Before;
import org.junit.Ignore;
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

public class ImportWithSplittingTest {

    private OTeleporterContext context;
    private ODBMSNaiveStrategy naiveStrategy;
    private String dbParentDirectoryPath;
    private final String configPathJson = "src/test/resources/configuration-mapping/splitting-into2tables-mapping.json";
    private ODBQueryEngine dbQueryEngine;
    private String driver = "org.hsqldb.jdbc.JDBCDriver";
    private String jurl = "jdbc:hsqldb:mem:mydb";
    private String username = "SA";
    private String password = "";
    private String outOrientGraphUri;
    private OSourceDatabaseInfo sourceDBInfo;


    @Before
    public void init() {
        this.context = OTeleporterContext.newInstance();
        this.dbQueryEngine = new ODBQueryEngine(this.driver);
        this.context.setDbQueryEngine(this.dbQueryEngine);
        this.context.setOutputManager(new OOutputStreamManager(0));
        this.context.setNameResolver(new OJavaConventionNameResolver());
        this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
        this.naiveStrategy = new ODBMSNaiveStrategy();
        this.outOrientGraphUri = "plocal:target/testOrientDB";
        this.dbParentDirectoryPath = this.outOrientGraphUri.replace("plocal:","");
        this.sourceDBInfo = new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
    }

    @Ignore
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
        OrientGraphNoTx orientGraph = null;

        try {

            Class.forName(this.driver);
            connection = DriverManager.getConnection(this.jurl, this.username, this.password);

            String employeeTableBuilding = "create memory table EMPLOYEE_PROJECT (FIRST_NAME varchar(256) not null," +
                    " LAST_NAME varchar(256) not null, SALARY double not null, DEPARTMENT varchar(256) not null," +
                    " PROJECT varchar(256) not null, BALANCE double not null, ROLE varchar(256), primary key (FIRST_NAME,LAST_NAME,PROJECT))";
            st = connection.createStatement();
            st.execute(employeeTableBuilding);

            String departmentTableBuilding = "create memory table DEPARTMENT (ID varchar(256),"+
                    " NAME varchar(256) not null, LOCATION varchar(256) not null, UPDATED_ON date not null, primary key (ID))";
            st.execute(departmentTableBuilding);


            // Records Inserting

            String personFilling = "insert into EMPLOYEE (FIRST_NAME,LAST_NAME,SALARY,DEPARTMENT,PROJECT,BALANCE,ROLE) values ("
                    + "('Joe','Black','20000','D001','Mars','12000','T'),"
                    + "('Thomas','Anderson','35000','D002','Venus','15000','T'),"
                    + "('Tyler','Durden','35000','D001','Iuppiter','20000','A'),"
                    + "('John','McClanenei','25000','D001','Venus','15000','S'),"
                    + "('Marty','McFly','40000','D002','Mars','12000','M'),"
                    + "('Marty','McFly','40000','D002','Mercury','5000','M'))";
            st.execute(personFilling);

            String departmentFilling = "insert into DEPARTMENT (ID,NAME,LOCATION,UPDATED_ON) values ("
                    + "('D001','Data Migration','London','2016-05-10'),"
                    + "('D002','Contracts Update','Glasgow','2016-05-10'))";
            st.execute(departmentFilling);

            ODocument configDoc = OMigrationConfigManager.loadMigrationConfigFromFile(this.configPathJson);

            this.naiveStrategy
                    .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, configDoc);


            /**
             *  Testing context information
             */

            assertEquals(8, context.getStatistics().totalNumberOfRecords);
            assertEquals(8, context.getStatistics().analyzedRecords);
            assertEquals(11, context.getStatistics().orientAddedVertices);
            assertEquals(11, context.getStatistics().orientAddedEdges);


            /**
             *  Testing built OrientDB
             */
            orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

            // vertices check

            int count = 0;
            for(Vertex v: orientGraph.getVertices()) {
                assertNotNull(v.getId());
                count++;
            }
            assertEquals(11, count);

            count = 0;
            for(Vertex v: orientGraph.getVerticesOfClass("Employee")) {
                assertNotNull(v.getId());
                count++;
            }
            assertEquals(5, count);

            count = 0;
            for(Vertex v: orientGraph.getVerticesOfClass("Project")) {
                assertNotNull(v.getId());
                count++;
            }
            assertEquals(4, count);

            count = 0;
            for(Vertex v: orientGraph.getVerticesOfClass("Department")) {
                assertNotNull(v.getId());
                count++;
            }
            assertEquals(2, count);

            // edges check
            count = 0;
            for(Edge e: orientGraph.getEdges()) {
                assertNotNull(e.getId());
                count++;
            }
            assertEquals(11, count);

            count = 0;
            for(Edge e: orientGraph.getEdgesOfClass("WorksAt")) {
                assertNotNull(e.getId());
                count++;
            }
            assertEquals(5, count);

            count = 0;
            for(Edge e: orientGraph.getEdgesOfClass("HasProject")) {
                assertNotNull(e.getId());
                count++;
            }
            assertEquals(6, count);


            // vertex properties and connections check
            Iterator<Edge> edgesIt = null;
            String[] keys = {"id"};
            String[] values = {"D001"};

            Vertex v = null;
            Iterator<Vertex> iterator = orientGraph.getVertices("Department", keys, values).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("D001", v.getProperty("id"));
                assertEquals("Data Migration", v.getProperty("departmentName"));
                assertEquals("London", v.getProperty("location"));
                assertNull(v.getProperty("updatedOn"));
                edgesIt = v.getEdges(Direction.IN, "WorksAt").iterator();
                assertEquals("Black", edgesIt.next().getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals("Durden", edgesIt.next().getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals("McClanenei", edgesIt.next().getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

            values[0] = "D002";
            iterator = orientGraph.getVertices("Department", keys, values).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("D002", v.getProperty("id"));
                assertEquals("Contracts Update", v.getProperty("departmentName"));
                assertEquals("Glasgow", v.getProperty("location"));
                assertNull(v.getProperty("updatedOn"));
                edgesIt = v.getEdges(Direction.IN, "WorksAt").iterator();
                assertEquals("Anderson", edgesIt.next().getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals("McFly", edgesIt.next().getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

            String[] employeeKeys = {"firstName","lastName"};
            String[] employeeValues = {"Joe","Black"};
            iterator = orientGraph.getVertices("Employee", employeeKeys, employeeValues).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("Joe", v.getProperty("firstName"));
                assertEquals("Black", v.getProperty("lastName"));
                assertEquals(20000, v.getProperty("salary"));
                assertEquals("D001", v.getProperty("department"));
                assertNull(v.getProperty("updatedOn"));
                edgesIt = v.getEdges(Direction.OUT, "WorksAt").iterator();
                assertEquals("D001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
                assertEquals(false, edgesIt.hasNext());
                edgesIt = v.getEdges(Direction.OUT, "HasProject").iterator();
                Edge currentSplittingEdge = edgesIt.next();
                assertEquals("Mars", currentSplittingEdge.getVertex(Direction.IN).getProperty("project"));
                assertEquals("T", currentSplittingEdge.getProperty("role"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

            employeeValues[0] = "Thomas";
            employeeValues[1] = "Anderson";
            iterator = orientGraph.getVertices("Person", employeeKeys, employeeValues).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("P002", v.getProperty("extKey1"));
                assertEquals("Thomas", v.getProperty("firstName"));
                assertEquals("Anderson", v.getProperty("lastName"));
                assertNull(v.getProperty("depId"));
                assertEquals("P002", v.getProperty("extKey2"));
                assertEquals("627390164", v.getProperty("VAT"));
                assertNull(v.getProperty("updatedOn"));
                edgesIt = v.getEdges(Direction.OUT, "WorksAt").iterator();
                assertEquals("D002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
                assertEquals(false, edgesIt.hasNext());
                edgesIt = v.getEdges(Direction.OUT, "HasProject").iterator();
                Edge currentSplittingEdge = edgesIt.next();
                assertEquals("Venus", currentSplittingEdge.getVertex(Direction.IN).getProperty("project"));
                assertEquals("T", currentSplittingEdge.getProperty("role"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

            employeeValues[0] = "Tyler";
            employeeValues[1] = "Durden";
            iterator = orientGraph.getVertices("Person", employeeKeys, employeeValues).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("P003", v.getProperty("extKey1"));
                assertEquals("Tyler", v.getProperty("firstName"));
                assertEquals("Durden", v.getProperty("lastName"));
                assertNull(v.getProperty("depId"));
                assertEquals("P003", v.getProperty("extKey2"));
                assertEquals("472889102", v.getProperty("VAT"));
                assertNull(v.getProperty("updatedOn"));
                edgesIt = v.getEdges(Direction.OUT, "WorksAt").iterator();
                assertEquals("D001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
                assertEquals(false, edgesIt.hasNext());
                edgesIt = v.getEdges(Direction.OUT, "HasProject").iterator();
                Edge currentSplittingEdge = edgesIt.next();
                assertEquals("Iuppiter", currentSplittingEdge.getVertex(Direction.IN).getProperty("project"));
                assertEquals("A", currentSplittingEdge.getProperty("role"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

            employeeValues[0] = "John";
            employeeValues[1] = "McClanenei";
            iterator = orientGraph.getVertices("Person", employeeKeys, employeeValues).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("P004", v.getProperty("extKey1"));
                assertEquals("John", v.getProperty("firstName"));
                assertEquals("McClanenei", v.getProperty("lastName"));
                assertNull(v.getProperty("depId"));
                assertEquals("P004", v.getProperty("extKey2"));
                assertEquals("564856410", v.getProperty("VAT"));
                assertNull(v.getProperty("updatedOn"));
                edgesIt = v.getEdges(Direction.OUT, "WorksAt").iterator();
                assertEquals("D001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
                assertEquals(false, edgesIt.hasNext());
                edgesIt = v.getEdges(Direction.OUT, "HasProject").iterator();
                Edge currentSplittingEdge = edgesIt.next();
                assertEquals("Venus", currentSplittingEdge.getVertex(Direction.IN).getProperty("project"));
                assertEquals("S", currentSplittingEdge.getProperty("role"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

            employeeValues[0] = "Marty";
            employeeValues[1] = "McFly";
            iterator = orientGraph.getVertices("Person", employeeKeys, employeeValues).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("P005", v.getProperty("extKey1"));
                assertEquals("Ellen", v.getProperty("firstName"));
                assertEquals("Ripley", v.getProperty("lastName"));
                assertNull(v.getProperty("depId"));
                assertEquals("P005", v.getProperty("extKey2"));
                assertEquals("467280751", v.getProperty("VAT"));
                assertNull(v.getProperty("updatedOn"));
                edgesIt = v.getEdges(Direction.OUT, "WorksAt").iterator();
                assertEquals("D002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
                assertEquals(false, edgesIt.hasNext());
                edgesIt = v.getEdges(Direction.OUT, "HasProject").iterator();
                Edge currentSplittingEdge = edgesIt.next();
                assertEquals("Mars", currentSplittingEdge.getVertex(Direction.IN).getProperty("project"));
                assertEquals("M", currentSplittingEdge.getProperty("role"));
                currentSplittingEdge = edgesIt.next();
                assertEquals("Mercury", currentSplittingEdge.getVertex(Direction.IN).getProperty("project"));
                assertEquals("M", currentSplittingEdge.getProperty("role"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

            String[] projectKeys = {"project"};
            String[] projectValues = {"Mars"};
            iterator = orientGraph.getVertices("Project", projectKeys, projectValues).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("Mars", v.getProperty("project"));
                assertEquals(12000, v.getProperty("balance"));
                edgesIt = v.getEdges(Direction.OUT, "WorksAt").iterator();
                assertEquals("D001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
                assertEquals(false, edgesIt.hasNext());
                edgesIt = v.getEdges(Direction.IN, "HasProject").iterator();
                Edge currentSplittingEdge = edgesIt.next();
                assertEquals("Black", currentSplittingEdge.getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals("T", currentSplittingEdge.getProperty("role"));
                currentSplittingEdge = edgesIt.next();
                assertEquals("McFly", currentSplittingEdge.getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals("M", currentSplittingEdge.getProperty("role"));
                assertEquals(false, edgesIt.hasNext());

            }
            else {
                fail("Query fail!");
            }

            projectValues[0] = "Venus";
            iterator = orientGraph.getVertices("Project", projectKeys, projectValues).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("Venus", v.getProperty("project"));
                assertEquals(15000, v.getProperty("balance"));
                edgesIt = v.getEdges(Direction.OUT, "WorksAt").iterator();
                assertEquals("D001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
                assertEquals(false, edgesIt.hasNext());
                edgesIt = v.getEdges(Direction.IN, "HasProject").iterator();
                Edge currentSplittingEdge = edgesIt.next();
                assertEquals("Anderson", currentSplittingEdge.getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals("T", currentSplittingEdge.getProperty("role"));
                currentSplittingEdge = edgesIt.next();
                assertEquals("McClanenei", currentSplittingEdge.getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals("S", currentSplittingEdge.getProperty("role"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

            projectValues[0] = "Iuppiter";
            iterator = orientGraph.getVertices("Project", projectKeys, projectValues).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("Iuppiter", v.getProperty("project"));
                assertEquals(20000, v.getProperty("balance"));
                edgesIt = v.getEdges(Direction.OUT, "WorksAt").iterator();
                assertEquals("D001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
                assertEquals(false, edgesIt.hasNext());
                edgesIt = v.getEdges(Direction.IN, "HasProject").iterator();
                Edge currentSplittingEdge = edgesIt.next();
                assertEquals("Durden", currentSplittingEdge.getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals("A", currentSplittingEdge.getProperty("role"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

            projectValues[0] = "Mercury";
            iterator = orientGraph.getVertices("Project", projectKeys, projectValues).iterator();
            assertTrue(iterator.hasNext());
            if(iterator.hasNext()) {
                v = iterator.next();
                assertEquals("Mercury", v.getProperty("project"));
                assertEquals(5000, v.getProperty("balance"));
                edgesIt = v.getEdges(Direction.OUT, "WorksAt").iterator();
                assertEquals("D001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
                assertEquals(false, edgesIt.hasNext());
                edgesIt = v.getEdges(Direction.IN, "HasProject").iterator();
                Edge currentSplittingEdge = edgesIt.next();
                assertEquals("McFly", currentSplittingEdge.getVertex(Direction.OUT).getProperty("lastName"));
                assertEquals("M", currentSplittingEdge.getProperty("role"));
                assertEquals(false, edgesIt.hasNext());
            }
            else {
                fail("Query fail!");
            }

        }catch(Exception e) {
            e.printStackTrace();
            fail();
        }finally {
            try {

                // Dropping Source DB Schema and OrientGraph
                String dbDropping = "drop schema public cascade";
                st.execute(dbDropping);
                connection.close();
                OFileManager.deleteResource(this.dbParentDirectoryPath);
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
