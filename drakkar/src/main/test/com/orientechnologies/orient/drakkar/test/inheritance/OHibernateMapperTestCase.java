/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.drakkar.test.inheritance;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.OOutputStreamManager;
import com.orientechnologies.orient.drakkar.mapper.OER2GraphMapper;
import com.orientechnologies.orient.drakkar.mapper.OHibernate2GraphMapper;
import com.orientechnologies.orient.drakkar.model.dbschema.OEntity;
import com.orientechnologies.orient.drakkar.model.graphmodel.OVertexType;
import com.orientechnologies.orient.drakkar.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.orient.drakkar.persistence.handler.OHSQLDBDataTypeHandler;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OHibernateMapperTestCase {

  private OER2GraphMapper mapper;
  private ODrakkarContext context;

  private final static String XML_TABLE_PER_CLASS = "src/main/resources/inheritance/hibernate/tablePerClassHierarchyInheritanceTest.xml";
  private final static String XML_TABLE_PER_SUBCLASS1 = "src/main/resources/inheritance/hibernate/tablePerSubclassInheritanceTest1.xml";
  private final static String XML_TABLE_PER_SUBCLASS2 = "src/main/resources/inheritance/hibernate/tablePerSubclassInheritanceTest2.xml";
  private final static String XML_TABLE_PER_CONCRETE_CLASS = "src/main/resources/inheritance/hibernate/tablePerConcreteClassInheritanceTest.xml";


  @Before
  public void init() {
    this.context = new ODrakkarContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
  }
  
  
  @Test

  /*
   * Table per Class Hierarchy Inheritance (<subclass> tag)
   *  table ( http://www.javatpoint.com/hibernate-table-per-hierarchy-example-using-xml-file )
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " TYPE varchar(256), NAME varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), "
          + "primary key (id))";
      st = connection.createStatement();
      st.execute(employeeTableBuilding);

      this.mapper = new OHibernate2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", OHibernateMapperTestCase.XML_TABLE_PER_CLASS);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(1, context.getStatistics().totalNumberOfEntities);
//      assertEquals(3, context.getStatistics().builtEntities);
      assertEquals(0, context.getStatistics().detectedRelationships);

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(0, context.getStatistics().analizedRelationships);
      assertEquals(0, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema 
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("CONTRACT_EMPLOYEE");


      // entities check
      assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(0, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(regularEmployeeEntity);
      assertNotNull(contractEmployeeEntity);


      // attributes check
      assertEquals(3, employeeEntity.getAttributes().size());

      assertNotNull(employeeEntity.getAttributeByName("ID"));
      assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("TYPE"));
      assertEquals("TYPE", employeeEntity.getAttributeByName("TYPE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("TYPE").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("TYPE").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("TYPE").getBelongingEntity().getName());
      
      assertNotNull(employeeEntity.getAttributeByName("NAME"));
      assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertEquals(2, regularEmployeeEntity.getAttributes().size());

//      assertNotNull(regularEmployeeEntity.getAttributeByName("EID"));
//      assertEquals("EID", regularEmployeeEntity.getAttributeByName("EID").getName());
//      assertEquals("VARCHAR", regularEmployeeEntity.getAttributeByName("EID").getDataType());
//      assertEquals(1, regularEmployeeEntity.getAttributeByName("EID").getOrdinalPosition());
//      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("EID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals("Regular_Employee", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals("Regular_Employee", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(2, contractEmployeeEntity.getAttributes().size());

//      assertNotNull(contractEmployeeEntity.getAttributeByName("EID"));
//      assertEquals("EID", contractEmployeeEntity.getAttributeByName("EID").getName());
//      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("EID").getDataType());
//      assertEquals(1, contractEmployeeEntity.getAttributeByName("EID").getOrdinalPosition());
//      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("EID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals("Contract_Employee", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals("Contract_Employee", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(0, regularEmployeeEntity.getRelationships().size());
      assertEquals(0, contractEmployeeEntity.getRelationships().size());
      assertEquals(0, employeeEntity.getRelationships().size());
      assertEquals(0, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(0, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(0, employeeEntity.getForeignKeys().size());

//      assertEquals("EMPLOYEE", regularEmployeeEntity.getRelationships().get(0).getParentEntityName());
//      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getRelationships().get(0).getForeignEntityName());
//      assertEquals("EMPLOYEE", contractEmployeeEntity.getRelationships().get(0).getParentEntityName());
//      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getRelationships().get(0).getForeignEntityName());
//      assertEquals(employeeEntity.getPrimaryKey(), regularEmployeeEntity.getRelationships().get(0).getPrimaryKey());
//      assertEquals(regularEmployeeEntity.getForeignKeys().get(0), regularEmployeeEntity.getRelationships().get(0).getForeignKey());
//      assertEquals(employeeEntity.getPrimaryKey(), contractEmployeeEntity.getRelationships().get(0).getPrimaryKey());
//      assertEquals(contractEmployeeEntity.getForeignKeys().get(0), contractEmployeeEntity.getRelationships().get(0).getForeignKey());

      // inheritance check
      assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
      assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
      assertNull(employeeEntity.getParentEntity());

      assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
      assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
      assertEquals(0, employeeEntity.getInheritanceLevel());


      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexByName("RegularEmployee");
      OVertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexByName("ContractEmployee");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      // properties check
      assertEquals(3, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("id"));
      assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("type"));
      assertEquals("type", employeeVertexType.getPropertyByName("type").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("type").getPropertyType());
      assertEquals(2, employeeVertexType.getPropertyByName("type").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("type").isFromPrimaryKey());
      
      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(3, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());
//      assertEquals(2, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
      assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
      assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getPropertyType());
      assertEquals(1, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
      assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
      assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getPropertyType());
      assertEquals(2, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

      assertEquals(2, contractEmployeeVertexType.getProperties().size());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
      assertEquals("payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
      assertEquals("DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getPropertyType());
      assertEquals(1, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
      assertEquals("contractDuration", contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getPropertyByName("contractDuration").getPropertyType());
      assertEquals(2, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

      // edges check
      assertEquals(0, mapper.getGraphModel().getEdgesType().size());

      // inheritance check
      assertEquals(employeeVertexType, regularEmployeeVertexType.getParentType());
      assertEquals(employeeVertexType, contractEmployeeVertexType.getParentType());
      assertNull(employeeVertexType.getParentType());

      assertEquals(1, regularEmployeeVertexType.getInheritanceLevel());
      assertEquals(1, contractEmployeeVertexType.getInheritanceLevel());
      assertEquals(0, employeeVertexType.getInheritanceLevel());


    }catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "DROP SCHEMA PUBLIC CASCADE";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
  }



  @Test

  /*
   * Table per Subclass Inheritance (<joined-subclass> tag)
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-subclass )
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), primary key (id))";
      st = connection.createStatement();
      st.execute(employeeTableBuilding);


      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper = new OHibernate2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", OHibernateMapperTestCase.XML_TABLE_PER_SUBCLASS1);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfEntities);
      assertEquals(3, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().detectedRelationships);

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().analizedRelationships);
      assertEquals(0, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema 
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");


      // entities check
      assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(regularEmployeeEntity);
      assertNotNull(contractEmployeeEntity);


      // attributes check
      assertEquals(2, employeeEntity.getAttributes().size());

      assertNotNull(employeeEntity.getAttributeByName("ID"));
      assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("NAME"));
      assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertEquals(3, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("EID"));
      assertEquals("EID", regularEmployeeEntity.getAttributeByName("EID").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getAttributeByName("EID").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("EID").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("EID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(3, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(3, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("EID"));
      assertEquals("EID", contractEmployeeEntity.getAttributeByName("EID").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("EID").getDataType());
      assertEquals(1, contractEmployeeEntity.getAttributeByName("EID").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("EID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(2, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(3, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(1, regularEmployeeEntity.getRelationships().size());
      assertEquals(1, contractEmployeeEntity.getRelationships().size());
      assertEquals(0, employeeEntity.getRelationships().size());
      assertEquals(1, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(1, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(0, employeeEntity.getForeignKeys().size());

      assertEquals("EMPLOYEE", regularEmployeeEntity.getRelationships().get(0).getParentEntityName());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getRelationships().get(0).getForeignEntityName());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getRelationships().get(0).getParentEntityName());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getRelationships().get(0).getForeignEntityName());
      assertEquals(employeeEntity.getPrimaryKey(), regularEmployeeEntity.getRelationships().get(0).getPrimaryKey());
      assertEquals(regularEmployeeEntity.getForeignKeys().get(0), regularEmployeeEntity.getRelationships().get(0).getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), contractEmployeeEntity.getRelationships().get(0).getPrimaryKey());
      assertEquals(contractEmployeeEntity.getForeignKeys().get(0), contractEmployeeEntity.getRelationships().get(0).getForeignKey());

      // inheritance check
      assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
      assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
      assertNull(employeeEntity.getParentEntity());

      assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
      assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
      assertEquals(0, employeeEntity.getInheritanceLevel());


      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexByName("RegularEmployee");
      OVertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexByName("ContractEmployee");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      // properties check
      assertEquals(2, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("id"));
      assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());
//      assertEquals(2, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
      assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
      assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getPropertyType());
      assertEquals(2, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
      assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
      assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getPropertyType());
      assertEquals(3, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

      assertEquals(2, contractEmployeeVertexType.getProperties().size());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
      assertEquals("payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
      assertEquals("DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getPropertyType());
      assertEquals(2, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
      assertEquals("contractDuration", contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getPropertyByName("contractDuration").getPropertyType());
      assertEquals(3, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

      // edges check
      assertEquals(0, mapper.getGraphModel().getEdgesType().size());

      // inheritance check
      assertEquals(employeeVertexType, regularEmployeeVertexType.getParentType());
      assertEquals(employeeVertexType, contractEmployeeVertexType.getParentType());
      assertNull(employeeVertexType.getParentType());

      assertEquals(1, regularEmployeeVertexType.getInheritanceLevel());
      assertEquals(1, contractEmployeeVertexType.getInheritanceLevel());
      assertEquals(0, employeeVertexType.getInheritanceLevel());


    }catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "DROP SCHEMA PUBLIC CASCADE";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
  }


  @Test

  /*
   * Table per Subclass Inheritance (<subclass> <join/> </subclass> tags)
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-subclass )
   */

  public void test3() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), EMPLOYEE_TYPE varchar(256), primary key (id))";
      st = connection.createStatement();
      st.execute(employeeTableBuilding);


      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper = new OHibernate2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", OHibernateMapperTestCase.XML_TABLE_PER_SUBCLASS2);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfEntities);
      assertEquals(3, context.getStatistics().builtEntities);      
      assertEquals(2, context.getStatistics().detectedRelationships);

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().analizedRelationships);
      assertEquals(0, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema 
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");


      // entities check
      assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(regularEmployeeEntity);
      assertNotNull(contractEmployeeEntity);


      // attributes check
      assertEquals(3, employeeEntity.getAttributes().size());

      assertNotNull(employeeEntity.getAttributeByName("ID"));
      assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("NAME"));
      assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("EMPLOYEE_TYPE"));
      assertEquals("EMPLOYEE_TYPE", employeeEntity.getAttributeByName("EMPLOYEE_TYPE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("EMPLOYEE_TYPE").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("EMPLOYEE_TYPE").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("EMPLOYEE_TYPE").getBelongingEntity().getName());

      assertEquals(3, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("EID"));
      assertEquals("EID", regularEmployeeEntity.getAttributeByName("EID").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getAttributeByName("EID").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("EID").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("EID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(3, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(3, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("EID"));
      assertEquals("EID", contractEmployeeEntity.getAttributeByName("EID").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("EID").getDataType());
      assertEquals(1, contractEmployeeEntity.getAttributeByName("EID").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("EID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(2, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(3, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(1, regularEmployeeEntity.getRelationships().size());
      assertEquals(1, contractEmployeeEntity.getRelationships().size());
      assertEquals(0, employeeEntity.getRelationships().size());
      assertEquals(1, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(1, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(0, employeeEntity.getForeignKeys().size());

      assertEquals("EMPLOYEE", regularEmployeeEntity.getRelationships().get(0).getParentEntityName());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getRelationships().get(0).getForeignEntityName());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getRelationships().get(0).getParentEntityName());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getRelationships().get(0).getForeignEntityName());
      assertEquals(employeeEntity.getPrimaryKey(), regularEmployeeEntity.getRelationships().get(0).getPrimaryKey());
      assertEquals(regularEmployeeEntity.getForeignKeys().get(0), regularEmployeeEntity.getRelationships().get(0).getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), contractEmployeeEntity.getRelationships().get(0).getPrimaryKey());
      assertEquals(contractEmployeeEntity.getForeignKeys().get(0), contractEmployeeEntity.getRelationships().get(0).getForeignKey());

      // inheritance check
      assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
      assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
      assertNull(employeeEntity.getParentEntity());

      assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
      assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
      assertEquals(0, employeeEntity.getInheritanceLevel());


      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexByName("RegularEmployee");
      OVertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexByName("ContractEmployee");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      // properties check
      assertEquals(3, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("id"));
      assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("employeeType"));
      assertEquals("employeeType", employeeVertexType.getPropertyByName("employeeType").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("employeeType").getPropertyType());
      assertEquals(3, employeeVertexType.getPropertyByName("employeeType").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("employeeType").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
      assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
      assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getPropertyType());
      assertEquals(2, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
      assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
      assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getPropertyType());
      assertEquals(3, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

      assertEquals(2, contractEmployeeVertexType.getProperties().size());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
      assertEquals("payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
      assertEquals("DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getPropertyType());
      assertEquals(2, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
      assertEquals("contractDuration", contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getPropertyByName("contractDuration").getPropertyType());
      assertEquals(3, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

      // edges check
      assertEquals(0, mapper.getGraphModel().getEdgesType().size());

      // inheritance check
      assertEquals(employeeVertexType, regularEmployeeVertexType.getParentType());
      assertEquals(employeeVertexType, contractEmployeeVertexType.getParentType());
      assertNull(employeeVertexType.getParentType());

      assertEquals(1, regularEmployeeVertexType.getInheritanceLevel());
      assertEquals(1, contractEmployeeVertexType.getInheritanceLevel());
      assertEquals(0, employeeVertexType.getInheritanceLevel());



    }catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "DROP SCHEMA PUBLIC CASCADE";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
  }


  @Test

  /*
   * Table per Concrete Class Inheritance (<union-subclass> tag)
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-concrete-class )
   */

  public void test4() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), primary key (id))";
      st = connection.createStatement();
      st.execute(employeeTableBuilding);


      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper = new OHibernate2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", OHibernateMapperTestCase.XML_TABLE_PER_SUBCLASS1);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfEntities);
      assertEquals(3, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().detectedRelationships);

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().analizedRelationships);
      assertEquals(0, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema 
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");


      // entities check
      assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(regularEmployeeEntity);
      assertNotNull(contractEmployeeEntity);


      // attributes check
      assertEquals(2, employeeEntity.getAttributes().size());

      assertNotNull(employeeEntity.getAttributeByName("ID"));
      assertEquals("ID", employeeEntity.getAttributeByName("ID").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("ID").getDataType());
      assertEquals(1, employeeEntity.getAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("NAME"));
      assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertEquals(3, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("EID"));
      assertEquals("EID", regularEmployeeEntity.getAttributeByName("EID").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getAttributeByName("EID").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("EID").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("EID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(3, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(3, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("EID"));
      assertEquals("EID", contractEmployeeEntity.getAttributeByName("EID").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("EID").getDataType());
      assertEquals(1, contractEmployeeEntity.getAttributeByName("EID").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("EID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(2, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(3, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(1, regularEmployeeEntity.getRelationships().size());
      assertEquals(1, contractEmployeeEntity.getRelationships().size());
      assertEquals(0, employeeEntity.getRelationships().size());
      assertEquals(1, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(1, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(0, employeeEntity.getForeignKeys().size());

      assertEquals("EMPLOYEE", regularEmployeeEntity.getRelationships().get(0).getParentEntityName());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getRelationships().get(0).getForeignEntityName());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getRelationships().get(0).getParentEntityName());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getRelationships().get(0).getForeignEntityName());
      assertEquals(employeeEntity.getPrimaryKey(), regularEmployeeEntity.getRelationships().get(0).getPrimaryKey());
      assertEquals(regularEmployeeEntity.getForeignKeys().get(0), regularEmployeeEntity.getRelationships().get(0).getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), contractEmployeeEntity.getRelationships().get(0).getPrimaryKey());
      assertEquals(contractEmployeeEntity.getForeignKeys().get(0), contractEmployeeEntity.getRelationships().get(0).getForeignKey());

      // inheritance check
      assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
      assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
      assertNull(employeeEntity.getParentEntity());

      assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
      assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
      assertEquals(0, employeeEntity.getInheritanceLevel());


      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexByName("RegularEmployee");
      OVertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexByName("ContractEmployee");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);

      // properties check
      assertEquals(2, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("id"));
      assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());
//      assertEquals(2, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
      assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
      assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getPropertyType());
      assertEquals(2, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
      assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
      assertEquals("DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getPropertyType());
      assertEquals(3, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

      assertEquals(2, contractEmployeeVertexType.getProperties().size());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
      assertEquals("payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
      assertEquals("DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getPropertyType());
      assertEquals(2, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
      assertEquals("contractDuration", contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getPropertyByName("contractDuration").getPropertyType());
      assertEquals(3, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

      // edges check
      assertEquals(0, mapper.getGraphModel().getEdgesType().size());

      // inheritance check
      assertEquals(employeeVertexType, regularEmployeeVertexType.getParentType());
      assertEquals(employeeVertexType, contractEmployeeVertexType.getParentType());
      assertNull(employeeVertexType.getParentType());

      assertEquals(1, regularEmployeeVertexType.getInheritanceLevel());
      assertEquals(1, contractEmployeeVertexType.getInheritanceLevel());
      assertEquals(0, employeeVertexType.getInheritanceLevel());


    }catch(Exception e) {
      e.printStackTrace();
    }finally {      
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "DROP SCHEMA PUBLIC CASCADE";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
  }

}
