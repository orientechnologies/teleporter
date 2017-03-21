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

package com.orientechnologies.teleporter.test.rdbms.jointableaggregation;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OEVClassMapper;
import com.orientechnologies.teleporter.model.dbschema.OCanonicalRelationship;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveAggregationStrategy;
import com.orientechnologies.teleporter.util.OFileManager;
import com.orientechnologies.teleporter.util.OGraphCommands;
import org.junit.After;
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

public class AggregationStrategyTest {

  private OTeleporterContext            context;
  private ODBMSNaiveAggregationStrategy importStrategy;
  private ODBQueryEngine                dbQueryEngine;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private String dbName = "testOrientDB";
  private String outParentDirectory = "embedded:target/";
  private String outOrientGraphUri = this.outParentDirectory + this.dbName;
  private OSourceDatabaseInfo sourceDBInfo;


  @Before
  public void init() {
    this.importStrategy = new ODBMSNaiveAggregationStrategy("embedded", this.outParentDirectory, this.dbName);
    this.context = OTeleporterContext.newInstance(this.outParentDirectory);
    this.context.initOrientDBInstance(this.outOrientGraphUri);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
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
   * Aggregation Strategy Test: executing mapping
   */ public void test1() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      // Tables Building

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"
          + " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(employeeTableBuilding);

      String departmentTableBuilding =
          "create memory table DEPARTMENT (ID varchar(256) not null, NAME  varchar(256)," + " primary key (ID))";
      st.execute(departmentTableBuilding);

      String dept2empTableBuilding =
          "create memory table DEPT_EMP (DEPT_ID varchar(256) not null, EMP_ID  varchar(256), HIRING_YEAR varchar(256),"
              + " primary key (DEPT_ID,EMP_ID), foreign key (EMP_ID) references EMPLOYEE(ID), foreign key (DEPT_ID) references DEPARTMENT(ID))";
      st.execute(dept2empTableBuilding);

      String dept2managerTableBuilding = "create memory table DEPT_MANAGER (DEPT_ID varchar(256) not null, EMP_ID  varchar(256),"
          + " primary key (DEPT_ID,EMP_ID), foreign key (EMP_ID) references EMPLOYEE(ID), foreign key (DEPT_ID) references DEPARTMENT(ID))";
      st.execute(dept2managerTableBuilding);

      String branchTableBuilding = "create memory table BRANCH(BRANCH_ID varchar(256) not null, LOCATION  varchar(256),"
          + "DEPT varchar(256) not null, primary key (BRANCH_ID), foreign key (DEPT) references DEPARTMENT(ID))";
      st.execute(branchTableBuilding);

      OER2GraphMapper mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());


      /*
       *  Testing context information
       */

      assertEquals(5, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(5, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType departmentVertexType = mapper.getGraphModel().getVertexTypeByName("Department");
      OVertexType deptEmpVertexType = mapper.getGraphModel().getVertexTypeByName("DeptEmp");
      OVertexType deptManagerVertexType = mapper.getGraphModel().getVertexTypeByName("DeptManager");
      OVertexType branchVertexType = mapper.getGraphModel().getVertexTypeByName("Branch");
      OEdgeType deptEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasDept");
      OEdgeType empEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasEmp");

      // vertices check
      assertEquals(5, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(departmentVertexType);
      assertNotNull(deptEmpVertexType);
      assertNotNull(deptManagerVertexType);
      assertNotNull(branchVertexType);

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(deptEdgeType);
      assertNotNull(empEdgeType);
      assertEquals(3, deptEdgeType.getNumberRelationshipsRepresented());
      assertEquals(2, empEdgeType.getNumberRelationshipsRepresented());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(5, mapper.getVertexType2EVClassMappers().size());
      assertEquals(5, mapper.getEntity2EVClassMappers().size());

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
      OEVClassMapper employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
      assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
      assertEquals(employeeClassMapper.getEntity(), employeeEntity);
      assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

      assertEquals(3, employeeClassMapper.getAttribute2property().size());
      assertEquals(3, employeeClassMapper.getProperty2attribute().size());
      assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", employeeClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", employeeClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", employeeClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", employeeClassMapper.getProperty2attribute().get("lastName"));

      OEntity departmentEntity = mapper.getDataBaseSchema().getEntityByName("DEPARTMENT");
      assertEquals(1, mapper.getEVClassMappersByVertex(departmentVertexType).size());
      OEVClassMapper departmentClassMapper = mapper.getEVClassMappersByVertex(departmentVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(departmentEntity).size());
      assertEquals(departmentClassMapper, mapper.getEVClassMappersByEntity(departmentEntity).get(0));
      assertEquals(departmentClassMapper.getEntity(), departmentEntity);
      assertEquals(departmentClassMapper.getVertexType(), departmentVertexType);

      assertEquals(2, departmentClassMapper.getAttribute2property().size());
      assertEquals(2, departmentClassMapper.getProperty2attribute().size());
      assertEquals("id", departmentClassMapper.getAttribute2property().get("ID"));
      assertEquals("name", departmentClassMapper.getAttribute2property().get("NAME"));
      assertEquals("ID", departmentClassMapper.getProperty2attribute().get("id"));
      assertEquals("NAME", departmentClassMapper.getProperty2attribute().get("name"));

      OEntity branchEntity = mapper.getDataBaseSchema().getEntityByName("BRANCH");
      assertEquals(1, mapper.getEVClassMappersByVertex(branchVertexType).size());
      OEVClassMapper branchClassMapper = mapper.getEVClassMappersByVertex(branchVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(branchEntity).size());
      assertEquals(branchClassMapper, mapper.getEVClassMappersByEntity(branchEntity).get(0));
      assertEquals(branchClassMapper.getEntity(), branchEntity);
      assertEquals(branchClassMapper.getVertexType(), branchVertexType);

      assertEquals(3, branchClassMapper.getAttribute2property().size());
      assertEquals(3, branchClassMapper.getProperty2attribute().size());
      assertEquals("branchId", branchClassMapper.getAttribute2property().get("BRANCH_ID"));
      assertEquals("location", branchClassMapper.getAttribute2property().get("LOCATION"));
      assertEquals("dept", branchClassMapper.getAttribute2property().get("DEPT"));
      assertEquals("BRANCH_ID", branchClassMapper.getProperty2attribute().get("branchId"));
      assertEquals("LOCATION", branchClassMapper.getProperty2attribute().get("location"));
      assertEquals("DEPT", branchClassMapper.getProperty2attribute().get("dept"));

      OEntity deptEmpEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_EMP");
      assertEquals(1, mapper.getEVClassMappersByVertex(deptEmpVertexType).size());
      OEVClassMapper deptEmpClassMapper = mapper.getEVClassMappersByVertex(deptEmpVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(deptEmpEntity).size());
      assertEquals(deptEmpClassMapper, mapper.getEVClassMappersByEntity(deptEmpEntity).get(0));
      assertEquals(deptEmpClassMapper.getEntity(), deptEmpEntity);
      assertEquals(deptEmpClassMapper.getVertexType(), deptEmpVertexType);

      assertEquals(3, deptEmpClassMapper.getAttribute2property().size());
      assertEquals(3, deptEmpClassMapper.getProperty2attribute().size());
      assertEquals("deptId", deptEmpClassMapper.getAttribute2property().get("DEPT_ID"));
      assertEquals("empId", deptEmpClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("hiringYear", deptEmpClassMapper.getAttribute2property().get("HIRING_YEAR"));
      assertEquals("DEPT_ID", deptEmpClassMapper.getProperty2attribute().get("deptId"));
      assertEquals("EMP_ID", deptEmpClassMapper.getProperty2attribute().get("empId"));
      assertEquals("HIRING_YEAR", deptEmpClassMapper.getProperty2attribute().get("hiringYear"));

      OEntity deptMgrEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_MANAGER");
      assertEquals(1, mapper.getEVClassMappersByVertex(deptManagerVertexType).size());
      OEVClassMapper deptManagerClassMapper = mapper.getEVClassMappersByVertex(deptManagerVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(deptMgrEntity).size());
      assertEquals(deptManagerClassMapper, mapper.getEVClassMappersByEntity(deptMgrEntity).get(0));
      assertEquals(deptManagerClassMapper.getEntity(), deptMgrEntity);
      assertEquals(deptManagerClassMapper.getVertexType(), deptManagerVertexType);

      assertEquals(2, deptManagerClassMapper.getAttribute2property().size());
      assertEquals(2, deptManagerClassMapper.getProperty2attribute().size());
      assertEquals("deptId", deptManagerClassMapper.getAttribute2property().get("DEPT_ID"));
      assertEquals("empId", deptManagerClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("DEPT_ID", deptManagerClassMapper.getProperty2attribute().get("deptId"));
      assertEquals("EMP_ID", deptManagerClassMapper.getProperty2attribute().get("empId"));

      // Relationships-Edges Mapping

      Iterator<OCanonicalRelationship> it = deptEmpEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship hasDepartmentRelationship1 = it.next();
      OCanonicalRelationship hasEmployeeRelationship1 = it.next();
      assertFalse(it.hasNext());

      it = deptMgrEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship hasDepartmentRelationship2 = it.next();
      OCanonicalRelationship hasEmployeeRelationship2 = it.next();
      assertFalse(it.hasNext());

      it = branchEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship hasDepartmentRelationship3 = it.next();
      assertFalse(it.hasNext());

      assertEquals(5, mapper.getRelationship2edgeType().size());
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship1));
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship2));
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship3));
      assertEquals(empEdgeType, mapper.getRelationship2edgeType().get(hasEmployeeRelationship1));
      assertEquals(empEdgeType, mapper.getRelationship2edgeType().get(hasEmployeeRelationship2));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(3, mapper.getEdgeType2relationships().get(deptEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship1));
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship2));
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship3));
      assertEquals(2, mapper.getEdgeType2relationships().get(empEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship1));
      assertTrue(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship2));

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

      /*
       * Aggregation of join tables
       */
      mapper.performMany2ManyAggregation();


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(3, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(3, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */

      employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      departmentVertexType = mapper.getGraphModel().getVertexTypeByName("Department");
      branchVertexType = mapper.getGraphModel().getVertexTypeByName("Branch");
      deptEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasDept");
      OEdgeType deptEmpEdgeType = mapper.getGraphModel().getEdgeTypeByName("DeptEmp");
      OEdgeType deptManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("DeptManager");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(departmentVertexType);
      assertNull(mapper.getGraphModel().getVertexTypeByName("DeptEmp"));
      assertNull(mapper.getGraphModel().getVertexTypeByName("DeptManager"));
      assertNotNull(branchVertexType);

      // edges check
      assertEquals(3, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(deptEdgeType);
      assertNotNull(deptEmpEdgeType);
      assertNotNull(deptManagerEdgeType);
      assertNull(mapper.getGraphModel().getEdgeTypeByName("HasEmp"));
      assertEquals(1, deptEdgeType.getNumberRelationshipsRepresented());
      assertEquals(1, deptEmpEdgeType.getNumberRelationshipsRepresented());
      assertEquals(1, deptManagerEdgeType.getNumberRelationshipsRepresented());

      assertNotNull(deptEmpEdgeType.getPropertyByName("hiringYear"));
      assertTrue(deptEmpEdgeType.getPropertyByName("hiringYear").getOriginalType().equals("VARCHAR"));

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(5, mapper.getVertexType2EVClassMappers().size());
      assertEquals(5, mapper.getEntity2EVClassMappers().size());

      employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      assertEquals(1, mapper.getEVClassMappersByVertex(employeeVertexType).size());
      employeeClassMapper = mapper.getEVClassMappersByVertex(employeeVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(employeeEntity).size());
      assertEquals(employeeClassMapper, mapper.getEVClassMappersByEntity(employeeEntity).get(0));
      assertEquals(employeeClassMapper.getEntity(), employeeEntity);
      assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

      assertEquals(3, employeeClassMapper.getAttribute2property().size());
      assertEquals(3, employeeClassMapper.getProperty2attribute().size());
      assertEquals("id", employeeClassMapper.getAttribute2property().get("ID"));
      assertEquals("firstName", employeeClassMapper.getAttribute2property().get("FIRST_NAME"));
      assertEquals("lastName", employeeClassMapper.getAttribute2property().get("LAST_NAME"));
      assertEquals("ID", employeeClassMapper.getProperty2attribute().get("id"));
      assertEquals("FIRST_NAME", employeeClassMapper.getProperty2attribute().get("firstName"));
      assertEquals("LAST_NAME", employeeClassMapper.getProperty2attribute().get("lastName"));

      departmentEntity = mapper.getDataBaseSchema().getEntityByName("DEPARTMENT");
      assertEquals(1, mapper.getEVClassMappersByVertex(departmentVertexType).size());
      departmentClassMapper = mapper.getEVClassMappersByVertex(departmentVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(departmentEntity).size());
      assertEquals(departmentClassMapper, mapper.getEVClassMappersByEntity(departmentEntity).get(0));
      assertEquals(departmentClassMapper.getEntity(), departmentEntity);
      assertEquals(departmentClassMapper.getVertexType(), departmentVertexType);

      assertEquals(2, departmentClassMapper.getAttribute2property().size());
      assertEquals(2, departmentClassMapper.getProperty2attribute().size());
      assertEquals("id", departmentClassMapper.getAttribute2property().get("ID"));
      assertEquals("name", departmentClassMapper.getAttribute2property().get("NAME"));
      assertEquals("ID", departmentClassMapper.getProperty2attribute().get("id"));
      assertEquals("NAME", departmentClassMapper.getProperty2attribute().get("name"));

      branchEntity = mapper.getDataBaseSchema().getEntityByName("BRANCH");
      assertEquals(1, mapper.getEVClassMappersByVertex(branchVertexType).size());
      branchClassMapper = mapper.getEVClassMappersByVertex(branchVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(branchEntity).size());
      assertEquals(branchClassMapper, mapper.getEVClassMappersByEntity(branchEntity).get(0));
      assertEquals(branchClassMapper.getEntity(), branchEntity);
      assertEquals(branchClassMapper.getVertexType(), branchVertexType);

      assertEquals(3, branchClassMapper.getAttribute2property().size());
      assertEquals(3, branchClassMapper.getProperty2attribute().size());
      assertEquals("branchId", branchClassMapper.getAttribute2property().get("BRANCH_ID"));
      assertEquals("location", branchClassMapper.getAttribute2property().get("LOCATION"));
      assertEquals("dept", branchClassMapper.getAttribute2property().get("DEPT"));
      assertEquals("BRANCH_ID", branchClassMapper.getProperty2attribute().get("branchId"));
      assertEquals("LOCATION", branchClassMapper.getProperty2attribute().get("location"));
      assertEquals("DEPT", branchClassMapper.getProperty2attribute().get("dept"));

      deptEmpEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_EMP");
      assertEquals(1, mapper.getEVClassMappersByVertex(deptEmpVertexType).size());
      deptEmpClassMapper = mapper.getEVClassMappersByVertex(deptEmpVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(deptEmpEntity).size());
      assertEquals(deptEmpClassMapper, mapper.getEVClassMappersByEntity(deptEmpEntity).get(0));
      assertEquals(deptEmpClassMapper.getEntity(), deptEmpEntity);
      assertEquals(deptEmpClassMapper.getVertexType(), deptEmpVertexType);

      assertEquals(3, deptEmpClassMapper.getAttribute2property().size());
      assertEquals(3, deptEmpClassMapper.getProperty2attribute().size());
      assertEquals("deptId", deptEmpClassMapper.getAttribute2property().get("DEPT_ID"));
      assertEquals("empId", deptEmpClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("hiringYear", deptEmpClassMapper.getAttribute2property().get("HIRING_YEAR"));
      assertEquals("DEPT_ID", deptEmpClassMapper.getProperty2attribute().get("deptId"));
      assertEquals("EMP_ID", deptEmpClassMapper.getProperty2attribute().get("empId"));
      assertEquals("HIRING_YEAR", deptEmpClassMapper.getProperty2attribute().get("hiringYear"));

      deptMgrEntity = mapper.getDataBaseSchema().getEntityByName("DEPT_MANAGER");
      assertEquals(1, mapper.getEVClassMappersByVertex(deptManagerVertexType).size());
      deptManagerClassMapper = mapper.getEVClassMappersByVertex(deptManagerVertexType).get(0);
      assertEquals(1, mapper.getEVClassMappersByEntity(deptMgrEntity).size());
      assertEquals(deptManagerClassMapper, mapper.getEVClassMappersByEntity(deptMgrEntity).get(0));
      assertEquals(deptManagerClassMapper.getEntity(), deptMgrEntity);
      assertEquals(deptManagerClassMapper.getVertexType(), deptManagerVertexType);

      assertEquals(2, deptManagerClassMapper.getAttribute2property().size());
      assertEquals(2, deptManagerClassMapper.getProperty2attribute().size());
      assertEquals("deptId", deptManagerClassMapper.getAttribute2property().get("DEPT_ID"));
      assertEquals("empId", deptManagerClassMapper.getAttribute2property().get("EMP_ID"));
      assertEquals("DEPT_ID", deptManagerClassMapper.getProperty2attribute().get("deptId"));
      assertEquals("EMP_ID", deptManagerClassMapper.getProperty2attribute().get("empId"));

      // Relationships-Edges Mapping

      it = deptEmpEntity.getOutCanonicalRelationships().iterator();
      hasDepartmentRelationship1 = it.next();
      hasEmployeeRelationship1 = it.next();
      assertFalse(it.hasNext());

      it = deptMgrEntity.getOutCanonicalRelationships().iterator();
      hasDepartmentRelationship2 = it.next();
      hasEmployeeRelationship2 = it.next();
      assertFalse(it.hasNext());

      it = branchEntity.getOutCanonicalRelationships().iterator();
      hasDepartmentRelationship3 = it.next();
      assertFalse(it.hasNext());

      // fetching empEdgeType from the rules as was deleted from the graph model during the aggregation
      assertEquals("HasEmp", empEdgeType.getName());
      assertEquals(employeeVertexType, empEdgeType.getInVertexType());
      assertEquals(0, empEdgeType.getAllProperties().size());

      assertEquals(5, mapper.getRelationship2edgeType().size());
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship1));
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship2));
      assertEquals(deptEdgeType, mapper.getRelationship2edgeType().get(hasDepartmentRelationship3));
      assertEquals(empEdgeType, mapper.getRelationship2edgeType().get(hasEmployeeRelationship1));
      assertEquals(empEdgeType, mapper.getRelationship2edgeType().get(hasEmployeeRelationship2));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(3, mapper.getEdgeType2relationships().get(deptEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship1));
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship2));
      assertTrue(mapper.getEdgeType2relationships().get(deptEdgeType).contains(hasDepartmentRelationship3));
      assertEquals(2, mapper.getEdgeType2relationships().get(empEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship1));
      assertTrue(mapper.getEdgeType2relationships().get(empEdgeType).contains(hasEmployeeRelationship2));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(2, mapper.getJoinVertex2aggregatorEdges().size());
      assertTrue(mapper.getJoinVertex2aggregatorEdges().containsKey(deptManagerVertexType));
      assertTrue(mapper.getJoinVertex2aggregatorEdges().containsKey(deptEmpVertexType));
      assertEquals(deptManagerEdgeType, mapper.getJoinVertex2aggregatorEdges().get(deptManagerVertexType).getEdgeType());
      assertEquals("Department", mapper.getJoinVertex2aggregatorEdges().get(deptManagerVertexType).getOutVertexClassName());
      assertEquals("Employee", mapper.getJoinVertex2aggregatorEdges().get(deptManagerVertexType).getInVertexClassName());
      assertEquals(deptEmpEdgeType, mapper.getJoinVertex2aggregatorEdges().get(deptEmpVertexType).getEdgeType());
      assertEquals("Department", mapper.getJoinVertex2aggregatorEdges().get(deptEmpVertexType).getOutVertexClassName());
      assertEquals("Employee", mapper.getJoinVertex2aggregatorEdges().get(deptEmpVertexType).getInVertexClassName());

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
   * Aggregation Strategy Test: executing import
   */ public void test2() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      // Tables Building

      String filmTableBuilding =
          "create memory table film (id varchar(256) not null," + " title varchar(256) not null, primary key (id))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      String actorTableBuilding = "create memory table actor (id varchar(256) not null, name  varchar(256),"
          + " surname varchar(256) not null, primary key (id))";
      st.execute(actorTableBuilding);

      String film2actorTableBuilding =
          "create memory table film_actor (film_id varchar(256) not null, actor_id  varchar(256), PAYMENT integer, "
              + " primary key (film_id,actor_id), foreign key (film_id) references film(id), foreign key (actor_id) references actor(id))";
      st.execute(film2actorTableBuilding);

      // Records Inserting

      String filmFilling =
          "insert into film(id,title) values (" + "('F001','The Wolf Of Wall Street')," + "('F002','Shutter Island'),"
              + "('F003','The Departed')," + "('F004','Inception'))";
      st.execute(filmFilling);

      String actorFilling =
          "insert into actor (id,name,surname) values (" + "('A001','Leonardo','Di Caprio')," + "('A002','Matthew', 'McConaughey'),"
              + "('A003','Ben','Kingsley')," + "('A004','Mark','Ruffalo')," + "('A005','Jack','Nicholson'),"
              + "('A006','Matt','Damon')," + "('A007','Michael','Caine'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into film_actor (film_id,actor_id,payment) values (" + "('F001','A001','32000000'),"
          + "('F001','A002','20000000')," + "('F002','A001','28000000')," + "('F002','A003','18000000'),"
          + "('F002','A004','6000000')," + "('F003','A001','25000000')," + "('F003','A005','27000000'),"
          + "('F003','A006','14000000')," + "('F004','A001','30000000')," + "('F004','A007','12000000'))";
      st.execute(film2actorFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null);


      /*
       *  Testing context information
       */

      assertEquals(21, context.getStatistics().totalNumberOfRecords);
      assertEquals(21, context.getStatistics().analyzedRecords);
      assertEquals(11, context.getStatistics().orientAddedVertices);
      assertEquals(10, context.getStatistics().orientAddedEdges);

      /*
       *  Testing built OrientDB
       */


      this.context.initOrientDBInstance(outOrientGraphUri);
      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      // vertices check

      assertEquals(11, orientGraph.countClass("V"));
      assertEquals(4, orientGraph.countClass("Film"));
      assertEquals(7, orientGraph.countClass("Actor"));

      // edges check

      assertEquals(10, orientGraph.countClass("E"));
      assertEquals(10, orientGraph.countClass("FilmActor"));

      // vertex properties and connections check
      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "F001" };

      OVertex v = null;
      OEdge currentEdge;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("The Wolf Of Wall Street", v.getProperty("title"));
        edgesIt = v.getEdges(ODirection.IN, "FilmActor").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(32000000), currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A002", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(20000000), currentEdge.getProperty("payment"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F002";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F002", v.getProperty("id"));
        assertEquals("Shutter Island", v.getProperty("title"));
        edgesIt = v.getEdges(ODirection.IN, "FilmActor").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(28000000), currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A003", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(18000000), currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A004", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(6000000), currentEdge.getProperty("payment"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F003";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F003", v.getProperty("id"));
        assertEquals("The Departed", v.getProperty("title"));
        edgesIt = v.getEdges(ODirection.IN, "FilmActor").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(25000000), currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A005", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(27000000), currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A006", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(14000000), currentEdge.getProperty("payment"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F004";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F004", v.getProperty("id"));
        assertEquals("Inception", v.getProperty("title"));
        edgesIt = v.getEdges(ODirection.IN, "FilmActor").iterator();
        currentEdge = edgesIt.next();
        assertEquals("A001", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(30000000), currentEdge.getProperty("payment"));
        currentEdge = edgesIt.next();
        assertEquals("A007", currentEdge.getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(Integer.valueOf(12000000), currentEdge.getProperty("payment"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A001";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A001", v.getProperty("id"));
        assertEquals("Leonardo", v.getProperty("name"));
        assertEquals("Di Caprio", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.OUT, "FilmActor").iterator();
        assertEquals("F001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals("F002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals("F003", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals("F004", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A002";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A002", v.getProperty("id"));
        assertEquals("Matthew", v.getProperty("name"));
        assertEquals("McConaughey", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.OUT, "FilmActor").iterator();
        assertEquals("F001", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A003";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A003", v.getProperty("id"));
        assertEquals("Ben", v.getProperty("name"));
        assertEquals("Kingsley", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.OUT, "FilmActor").iterator();
        assertEquals("F002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A004";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A004", v.getProperty("id"));
        assertEquals("Mark", v.getProperty("name"));
        assertEquals("Ruffalo", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.OUT, "FilmActor").iterator();
        assertEquals("F002", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A005";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A005", v.getProperty("id"));
        assertEquals("Jack", v.getProperty("name"));
        assertEquals("Nicholson", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.OUT, "FilmActor").iterator();
        assertEquals("F003", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A006";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A006", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("name"));
        assertEquals("Damon", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.OUT, "FilmActor").iterator();
        assertEquals("F003", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A007";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A007", v.getProperty("id"));
        assertEquals("Michael", v.getProperty("name"));
        assertEquals("Caine", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.OUT, "FilmActor").iterator();
        assertEquals("F004", edgesIt.next().getVertex(ODirection.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

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
