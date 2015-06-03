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
import com.orientechnologies.orient.drakkar.model.graphmodel.OEdgeType;
import com.orientechnologies.orient.drakkar.model.graphmodel.OVertexType;
import com.orientechnologies.orient.drakkar.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.orient.drakkar.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.orient.drakkar.strategy.ONaiveImportStrategy;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OHibernateInheritanceTestCase {

  private OER2GraphMapper mapper;
  private ODrakkarContext context;
  private ONaiveImportStrategy importStrategy;
  private String outOrientGraphUri;

  private final static String HIBERNATE_XML_MAPPING_FILE = "src/main/resources/inheritance/hibernate/test1.xml";

  @Before
  public void init() {
    this.context = new ODrakkarContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.importStrategy = new ONaiveImportStrategy();
    this.outOrientGraphUri = "memory:testOrientDB";
  }

  @Test

  /*
   * Mapper and models test case:
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-subclass )
   */

  public void test() {

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

      this.mapper = new OHibernate2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", OHibernateInheritanceTestCase.HIBERNATE_XML_MAPPING_FILE);
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
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);

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
      OEdgeType isAsEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasEid");

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

      assertEquals(3, regularEmployeeVertexType.getProperties().size());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("eid"));
      assertEquals("eid", regularEmployeeVertexType.getPropertyByName("eid").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getPropertyByName("eid").getPropertyType());
      assertEquals(1, regularEmployeeVertexType.getPropertyByName("eid").getOrdinalPosition());
      assertEquals(true, regularEmployeeVertexType.getPropertyByName("eid").isFromPrimaryKey());

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
      
      assertEquals(3, contractEmployeeVertexType.getProperties().size());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("eid"));
      assertEquals("eid", contractEmployeeVertexType.getPropertyByName("eid").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getPropertyByName("eid").getPropertyType());
      assertEquals(1, contractEmployeeVertexType.getPropertyByName("eid").getOrdinalPosition());
      assertEquals(true, contractEmployeeVertexType.getPropertyByName("eid").isFromPrimaryKey());

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
      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(isAsEdgeType);

      assertEquals("HasEid", isAsEdgeType.getName());
      assertEquals(0, isAsEdgeType.getProperties().size());
      assertEquals("Employee", isAsEdgeType.getInVertexType().getName());
      
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
