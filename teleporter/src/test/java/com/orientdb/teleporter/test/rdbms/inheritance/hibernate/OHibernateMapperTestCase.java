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

package com.orientdb.teleporter.test.rdbms.inheritance.hibernate;

import com.orientdb.teleporter.context.OOutputStreamManager;
import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientdb.teleporter.mapper.rdbms.OHibernate2GraphMapper;
import com.orientdb.teleporter.model.dbschema.OEntity;
import com.orientdb.teleporter.model.dbschema.OHierarchicalBag;
import com.orientdb.teleporter.model.dbschema.ORelationship;
import com.orientdb.teleporter.model.graphmodel.OVertexType;
import com.orientdb.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientdb.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OHibernateMapperTestCase {

  private OER2GraphMapper mapper;
  private OTeleporterContext context;

  private final static String XML_TABLE_PER_CLASS = "src/test/resources/inheritance/hibernate/tablePerClassHierarchyInheritanceTest.xml";
  private final static String XML_TABLE_PER_SUBCLASS1 = "src/test/resources/inheritance/hibernate/tablePerSubclassInheritanceTest1.xml";
  private final static String XML_TABLE_PER_SUBCLASS2 = "src/test/resources/inheritance/hibernate/tablePerSubclassInheritanceTest2.xml";
  private final static String XML_TABLE_PER_CONCRETE_CLASS = "src/test/resources/inheritance/hibernate/tablePerConcreteClassInheritanceTest.xml";


  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.context.setQueryQuoteType("\"");
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

      String residence = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " TYPE varchar(256), NAME varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), RESIDENCE varchar(256),"
          + "primary key (id), foreign key (RESIDENCE) references RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      this.mapper = new OHibernate2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", OHibernateMapperTestCase.XML_TABLE_PER_CLASS, null, null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);
      assertEquals(1, context.getStatistics().detectedRelationships);

      assertEquals(4, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(1, context.getStatistics().analizedRelationships);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema 
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("CONTRACT_EMPLOYEE");
      OEntity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");


      // entities check
      assertEquals(4, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(1, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(regularEmployeeEntity);
      assertNotNull(contractEmployeeEntity);
      assertNotNull(residenceEntity);


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

      assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(2, regularEmployeeEntity.getAttributes().size());

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

      // inherited attributes check
      assertEquals(0, employeeEntity.getInheritedAttributes().size());

      assertEquals(3, regularEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(3, contractEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

      // primary key check
      assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals("ID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

      assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals("ID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(0, regularEmployeeEntity.getOutRelationships().size());
      assertEquals(0, contractEmployeeEntity.getOutRelationships().size());
      assertEquals(1, employeeEntity.getOutRelationships().size());
      assertEquals(0, residenceEntity.getOutRelationships().size());
      assertEquals(0, regularEmployeeEntity.getInRelationships().size());
      assertEquals(0, contractEmployeeEntity.getInRelationships().size());
      assertEquals(0, employeeEntity.getInRelationships().size());
      assertEquals(1, residenceEntity.getInRelationships().size());

      assertEquals(0, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(0, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(0, residenceEntity.getForeignKeys().size());


      Iterator<ORelationship> itEmp = employeeEntity.getOutRelationships().iterator();
      ORelationship currentEmpRel = itEmp.next();
      assertEquals("RESIDENCE", currentEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentEmpRel.getForeignEntityName());
      assertEquals(residenceEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
      assertFalse(itEmp.hasNext());

      Iterator<ORelationship> itRes = residenceEntity.getInRelationships().iterator();
      ORelationship currentResRel = itRes.next();
      assertEquals(currentEmpRel, currentResRel);

      // inherited relationships check
      assertEquals(1, regularEmployeeEntity.getInheritedOutRelationships().size());
      assertEquals(1, contractEmployeeEntity.getInheritedOutRelationships().size());
      assertEquals(0, employeeEntity.getInheritedOutRelationships().size());
      assertEquals(0, residenceEntity.getInheritedOutRelationships().size());

      Iterator<ORelationship> itRegEmp = regularEmployeeEntity.getInheritedOutRelationships().iterator();
      Iterator<ORelationship> itContEmp = contractEmployeeEntity.getInheritedOutRelationships().iterator();
      ORelationship currentRegEmpRel = itRegEmp.next();
      ORelationship currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentRegEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntityName());
      assertEquals("RESIDENCE", currentContEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntityName());
      assertEquals(residenceEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(1, currentRegEmpRel.getForeignKey().getInvolvedAttributes().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getForeignKey().getInvolvedAttributes().get(0).getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(1, currentContEmpRel.getForeignKey().getInvolvedAttributes().size());
      assertEquals("RESIDENCE", currentContEmpRel.getForeignKey().getInvolvedAttributes().get(0).getName());
      assertFalse(itRegEmp.hasNext());
      assertFalse(itContEmp.hasNext());

      // inheritance check
      assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
      assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
      assertNull(employeeEntity.getParentEntity());
      assertNull(residenceEntity.getParentEntity());

      assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
      assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
      assertEquals(0, employeeEntity.getInheritanceLevel());
      assertEquals(0, residenceEntity.getInheritanceLevel());

      // Hierarchical Bag check
      assertEquals(1, mapper.getDataBaseSchema().getHierarchicalBags().size());
      OHierarchicalBag hierarchicalBag = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      assertEquals("table-per-hierarchy", hierarchicalBag.getInheritancePattern());

      assertEquals(2, hierarchicalBag.getDepth2entities().size());

      assertEquals(1, hierarchicalBag.getDepth2entities().get(0).size());
      Iterator<OEntity> it = hierarchicalBag.getDepth2entities().get(0).iterator();
      assertEquals("EMPLOYEE", it.next().getName());
      assertTrue(!it.hasNext());

      assertEquals(2, hierarchicalBag.getDepth2entities().get(1).size());
      it = hierarchicalBag.getDepth2entities().get(1).iterator();
      assertEquals("Regular_Employee", it.next().getName());
      assertEquals("Contract_Employee", it.next().getName());
      assertTrue(!it.hasNext());

      assertEquals(hierarchicalBag, employeeEntity.getHierarchicalBag());
      assertEquals(hierarchicalBag, regularEmployeeEntity.getHierarchicalBag());
      assertEquals(hierarchicalBag, contractEmployeeEntity.getHierarchicalBag());

      assertNotNull(hierarchicalBag.getDiscriminatorColumn());
      assertEquals("TYPE",hierarchicalBag.getDiscriminatorColumn());

      assertEquals(3, hierarchicalBag.getEntityName2discriminatorValue().size());
      assertEquals("emp",hierarchicalBag.getEntityName2discriminatorValue().get("EMPLOYEE"));
      assertEquals("reg_emp",hierarchicalBag.getEntityName2discriminatorValue().get("Regular_Employee"));
      assertEquals("cont_emp",hierarchicalBag.getEntityName2discriminatorValue().get("Contract_Employee"));


      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexByName("RegularEmployee");
      OVertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexByName("ContractEmployee");
      OVertexType residenceVertexType = mapper.getGraphModel().getVertexByName("Residence");


      // vertices check
      assertEquals(4, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);
      assertNotNull(residenceVertexType);

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

      assertNotNull(employeeVertexType.getPropertyByName("residence"));
      assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getPropertyType());
      assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());

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

      assertEquals(3, residenceVertexType.getProperties().size());

      assertNotNull(residenceVertexType.getPropertyByName("id"));
      assertEquals("id", residenceVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, residenceVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, residenceVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("city"));
      assertEquals("city", residenceVertexType.getPropertyByName("city").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("city").getPropertyType());
      assertEquals(2, residenceVertexType.getPropertyByName("city").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("city").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("country"));
      assertEquals("country", residenceVertexType.getPropertyByName("country").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("country").getPropertyType());
      assertEquals(3, residenceVertexType.getPropertyByName("country").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("country").isFromPrimaryKey());

      // inherited properties check
      assertEquals(0, employeeVertexType.getInheritedProperties().size());

      assertEquals(3, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getPropertyType());
      assertEquals(1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(true, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("name").getPropertyType());
      assertEquals(2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals("residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("residence").getPropertyType());
      assertEquals(3, regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(3, contractEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getPropertyType());
      assertEquals(1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(true, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("name").getPropertyType());
      assertEquals(2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals("residence", contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("residence").getPropertyType());
      assertEquals(3, contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(0, residenceVertexType.getInheritedProperties().size());

      // edges check

      assertEquals(1, mapper.getRelationship2edgeType().size());

      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertEquals("HasResidence", mapper.getGraphModel().getEdgesType().get(0).getName());

      assertEquals(1, employeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", employeeVertexType.getOutEdgesType().get(0).getName());

      assertEquals(1, regularEmployeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", regularEmployeeVertexType.getOutEdgesType().get(0).getName());

      assertEquals(1, contractEmployeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", contractEmployeeVertexType.getOutEdgesType().get(0).getName());

      // inheritance check
      assertEquals(employeeVertexType, regularEmployeeVertexType.getParentType());
      assertEquals(employeeVertexType, contractEmployeeVertexType.getParentType());
      assertNull(employeeVertexType.getParentType());

      assertEquals(1, regularEmployeeVertexType.getInheritanceLevel());
      assertEquals(1, contractEmployeeVertexType.getInheritanceLevel());
      assertEquals(0, employeeVertexType.getInheritanceLevel());


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

      String residence = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), RESIDENCE varchar(256), primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper = new OHibernate2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", OHibernateMapperTestCase.XML_TABLE_PER_SUBCLASS1, null, null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(4, context.getStatistics().totalNumberOfEntities);
      assertEquals(4, context.getStatistics().builtEntities);
      assertEquals(3, context.getStatistics().detectedRelationships);

      assertEquals(4, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().analizedRelationships);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema 
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");
      OEntity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");


      // entities check
      assertEquals(4, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(3, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(regularEmployeeEntity);
      assertNotNull(contractEmployeeEntity);
      assertNotNull(residenceEntity);


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

      assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(2, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(2, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

      // inherited attributes check
      assertEquals(0, employeeEntity.getInheritedAttributes().size());

      assertEquals(3, regularEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(3, contractEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

      // primary key check
      assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals("EID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

      assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals("EID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());


      // relationship, primary and foreign key check
      assertEquals(1, regularEmployeeEntity.getOutRelationships().size());
      assertEquals(1, contractEmployeeEntity.getOutRelationships().size());
      assertEquals(1, employeeEntity.getOutRelationships().size());
      assertEquals(0, residenceEntity.getOutRelationships().size());
      assertEquals(0, regularEmployeeEntity.getInRelationships().size());
      assertEquals(0, contractEmployeeEntity.getInRelationships().size());
      assertEquals(2, employeeEntity.getInRelationships().size());
      assertEquals(1, residenceEntity.getInRelationships().size());      

      assertEquals(1, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(1, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(0, residenceEntity.getForeignKeys().size());

      Iterator<ORelationship> itEmp = employeeEntity.getOutRelationships().iterator();
      Iterator<ORelationship> itRegEmp = regularEmployeeEntity.getOutRelationships().iterator();
      Iterator<ORelationship> itContEmp = contractEmployeeEntity.getOutRelationships().iterator();
      ORelationship currentEmpRel = itEmp.next();
      ORelationship currentRegEmpRel = itRegEmp.next();
      ORelationship currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentEmpRel.getForeignEntityName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getParentEntityName());
      assertEquals("REGULAR_EMPLOYEE", currentRegEmpRel.getForeignEntityName());
      assertEquals("EMPLOYEE", currentContEmpRel.getParentEntityName());
      assertEquals("CONTRACT_EMPLOYEE", currentContEmpRel.getForeignEntityName());
      assertEquals(residenceEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(regularEmployeeEntity.getForeignKeys().get(0), currentRegEmpRel.getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(contractEmployeeEntity.getForeignKeys().get(0), currentContEmpRel.getForeignKey());
      assertFalse(itEmp.hasNext());
      assertFalse(itRegEmp.hasNext());
      assertFalse(itContEmp.hasNext());

      Iterator<ORelationship> itRes = residenceEntity.getInRelationships().iterator();
      ORelationship currentResRel = itRes.next();
      assertEquals(currentEmpRel, currentResRel);

      itEmp = employeeEntity.getInRelationships().iterator();
      currentEmpRel = itEmp.next();
      assertEquals(currentEmpRel, currentContEmpRel);

      currentEmpRel = itEmp.next();
      assertEquals(currentEmpRel, currentRegEmpRel);


      // inherited relationships check
      assertEquals(1, regularEmployeeEntity.getInheritedOutRelationships().size());
      assertEquals(1, contractEmployeeEntity.getInheritedOutRelationships().size());
      assertEquals(0, employeeEntity.getInheritedOutRelationships().size());
      assertEquals(0, residenceEntity.getInheritedOutRelationships().size());

      itRegEmp = regularEmployeeEntity.getInheritedOutRelationships().iterator();
      itContEmp = contractEmployeeEntity.getInheritedOutRelationships().iterator();
      currentRegEmpRel = itRegEmp.next();
      currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentRegEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntityName());
      assertEquals("RESIDENCE", currentContEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntityName());
      assertEquals(residenceEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(1, currentRegEmpRel.getForeignKey().getInvolvedAttributes().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getForeignKey().getInvolvedAttributes().get(0).getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(1, currentContEmpRel.getForeignKey().getInvolvedAttributes().size());
      assertEquals("RESIDENCE", currentContEmpRel.getForeignKey().getInvolvedAttributes().get(0).getName());
      assertFalse(itRegEmp.hasNext());
      assertFalse(itContEmp.hasNext());

      assertEquals(1, currentRegEmpRel.getForeignKey().getInvolvedAttributes().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getForeignKey().getInvolvedAttributes().get(0).getName());

      // inheritance check
      assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
      assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
      assertNull(employeeEntity.getParentEntity());
      assertNull(residenceEntity.getParentEntity());

      assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
      assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
      assertEquals(0, employeeEntity.getInheritanceLevel());
      assertEquals(0, residenceEntity.getInheritanceLevel());

      // Hierarchical Bag check
      assertEquals(1, mapper.getDataBaseSchema().getHierarchicalBags().size());
      OHierarchicalBag hierarchicalBag = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      assertEquals("table-per-type", hierarchicalBag.getInheritancePattern());

      assertEquals(2, hierarchicalBag.getDepth2entities().size());

      assertEquals(1, hierarchicalBag.getDepth2entities().get(0).size());
      Iterator<OEntity> it = hierarchicalBag.getDepth2entities().get(0).iterator();
      assertEquals("EMPLOYEE", it.next().getName());
      assertTrue(!it.hasNext());

      assertEquals(2, hierarchicalBag.getDepth2entities().get(1).size());
      it = hierarchicalBag.getDepth2entities().get(1).iterator();
      assertEquals("REGULAR_EMPLOYEE", it.next().getName());
      assertEquals("CONTRACT_EMPLOYEE", it.next().getName());
      assertTrue(!it.hasNext());

      assertEquals(hierarchicalBag, employeeEntity.getHierarchicalBag());
      assertEquals(hierarchicalBag, regularEmployeeEntity.getHierarchicalBag());
      assertEquals(hierarchicalBag, contractEmployeeEntity.getHierarchicalBag());

      assertNull(hierarchicalBag.getDiscriminatorColumn());


      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexByName("RegularEmployee");
      OVertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexByName("ContractEmployee");
      OVertexType residenceVertexType = mapper.getGraphModel().getVertexByName("Residence");


      // vertices check
      assertEquals(4, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);
      assertNotNull(residenceVertexType);


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

      assertNotNull(employeeVertexType.getPropertyByName("residence"));
      assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getPropertyType());
      assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());

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

      assertEquals(3, residenceVertexType.getProperties().size());

      assertNotNull(residenceVertexType.getPropertyByName("id"));
      assertEquals("id", residenceVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, residenceVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, residenceVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("city"));
      assertEquals("city", residenceVertexType.getPropertyByName("city").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("city").getPropertyType());
      assertEquals(2, residenceVertexType.getPropertyByName("city").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("city").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("country"));
      assertEquals("country", residenceVertexType.getPropertyByName("country").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("country").getPropertyType());
      assertEquals(3, residenceVertexType.getPropertyByName("country").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("country").isFromPrimaryKey());

      // inherited properties check
      assertEquals(0, employeeVertexType.getInheritedProperties().size());

      assertEquals(3, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getPropertyType());
      assertEquals(1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey()); 

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("name").getPropertyType());
      assertEquals(2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals("residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("residence").getPropertyType());
      assertEquals(3, regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(3, contractEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getPropertyType());
      assertEquals(1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("name").getPropertyType());
      assertEquals(2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals("residence", contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("residence").getPropertyType());
      assertEquals(3, contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(0, residenceVertexType.getInheritedProperties().size());

      // edges check

      assertEquals(1, mapper.getRelationship2edgeType().size());

      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertEquals("HasResidence", mapper.getGraphModel().getEdgesType().get(0).getName());

      assertEquals(1, employeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", employeeVertexType.getOutEdgesType().get(0).getName());

      assertEquals(1, regularEmployeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", regularEmployeeVertexType.getOutEdgesType().get(0).getName());

      assertEquals(1, contractEmployeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", contractEmployeeVertexType.getOutEdgesType().get(0).getName());

      // inheritance check
      assertEquals(employeeVertexType, regularEmployeeVertexType.getParentType());
      assertEquals(employeeVertexType, contractEmployeeVertexType.getParentType());
      assertNull(employeeVertexType.getParentType());

      assertEquals(1, regularEmployeeVertexType.getInheritanceLevel());
      assertEquals(1, contractEmployeeVertexType.getInheritanceLevel());
      assertEquals(0, employeeVertexType.getInheritanceLevel());


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


  @Test

  /*
   * Table per Subclass Inheritance (<subclass> <join/> </subclass> tags)
   * 3 tables, one parent and 2 children ( http://www.javatpoint.com/table-per-subclass )
   */

  public void test3() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String residence = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), RESIDENCE varchar(256), primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID))";
      st.execute(employeeTableBuilding);


      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
          + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
          + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper = new OHibernate2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", OHibernateMapperTestCase.XML_TABLE_PER_SUBCLASS2, null, null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(4, context.getStatistics().totalNumberOfEntities);
      assertEquals(4, context.getStatistics().builtEntities);
      assertEquals(3, context.getStatistics().detectedRelationships);

      assertEquals(4, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().analizedRelationships);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema 
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");
      OEntity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");


      // entities check
      assertEquals(4, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(3, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(regularEmployeeEntity);
      assertNotNull(contractEmployeeEntity);
      assertNotNull(residenceEntity);


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

      assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(2, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(2, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

      // inherited attributes check
      assertEquals(0, employeeEntity.getInheritedAttributes().size());

      assertEquals(3, regularEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(3, contractEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

      // primary key check
      assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals("EID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

      assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals("EID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(1, regularEmployeeEntity.getOutRelationships().size());
      assertEquals(1, contractEmployeeEntity.getOutRelationships().size());
      assertEquals(1, employeeEntity.getOutRelationships().size());
      assertEquals(0, residenceEntity.getOutRelationships().size());
      assertEquals(0, regularEmployeeEntity.getInRelationships().size());
      assertEquals(0, contractEmployeeEntity.getInRelationships().size());
      assertEquals(2, employeeEntity.getInRelationships().size());
      assertEquals(1, residenceEntity.getInRelationships().size());      

      assertEquals(1, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(1, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(0, residenceEntity.getForeignKeys().size());

      Iterator<ORelationship> itEmp = employeeEntity.getOutRelationships().iterator();
      Iterator<ORelationship> itRegEmp = regularEmployeeEntity.getOutRelationships().iterator();
      Iterator<ORelationship> itContEmp = contractEmployeeEntity.getOutRelationships().iterator();
      ORelationship currentEmpRel = itEmp.next();
      ORelationship currentRegEmpRel = itRegEmp.next();
      ORelationship currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentEmpRel.getForeignEntityName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getParentEntityName());
      assertEquals("REGULAR_EMPLOYEE", currentRegEmpRel.getForeignEntityName());
      assertEquals("EMPLOYEE", currentContEmpRel.getParentEntityName());
      assertEquals("CONTRACT_EMPLOYEE", currentContEmpRel.getForeignEntityName());
      assertEquals(residenceEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(regularEmployeeEntity.getForeignKeys().get(0), currentRegEmpRel.getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(contractEmployeeEntity.getForeignKeys().get(0), currentContEmpRel.getForeignKey());
      assertFalse(itEmp.hasNext());
      assertFalse(itRegEmp.hasNext());
      assertFalse(itContEmp.hasNext());

      Iterator<ORelationship> itRes = residenceEntity.getInRelationships().iterator();
      ORelationship currentResRel = itRes.next();
      assertEquals(currentEmpRel, currentResRel);

      itEmp = employeeEntity.getInRelationships().iterator();
      currentEmpRel = itEmp.next();
      assertEquals(currentEmpRel, currentContEmpRel);

      currentEmpRel = itEmp.next();
      assertEquals(currentEmpRel, currentRegEmpRel);


      // inherited relationships check
      assertEquals(1, regularEmployeeEntity.getInheritedOutRelationships().size());
      assertEquals(1, contractEmployeeEntity.getInheritedOutRelationships().size());
      assertEquals(0, employeeEntity.getInheritedOutRelationships().size());
      assertEquals(0, residenceEntity.getInheritedOutRelationships().size());

      itRegEmp = regularEmployeeEntity.getInheritedOutRelationships().iterator();
      itContEmp = contractEmployeeEntity.getInheritedOutRelationships().iterator();
      currentRegEmpRel = itRegEmp.next();
      currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentRegEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntityName());
      assertEquals("RESIDENCE", currentContEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntityName());
      assertEquals(residenceEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(1, currentRegEmpRel.getForeignKey().getInvolvedAttributes().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getForeignKey().getInvolvedAttributes().get(0).getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(1, currentContEmpRel.getForeignKey().getInvolvedAttributes().size());
      assertEquals("RESIDENCE", currentContEmpRel.getForeignKey().getInvolvedAttributes().get(0).getName());
      assertFalse(itRegEmp.hasNext());
      assertFalse(itContEmp.hasNext());

      // inheritance check
      assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
      assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
      assertNull(employeeEntity.getParentEntity());
      assertNull(residenceEntity.getParentEntity());

      assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
      assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
      assertEquals(0, employeeEntity.getInheritanceLevel());
      assertEquals(0, residenceEntity.getInheritanceLevel());

      // Hierarchical Bag check
      assertEquals(1, mapper.getDataBaseSchema().getHierarchicalBags().size());
      OHierarchicalBag hierarchicalBag = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      assertEquals("table-per-type", hierarchicalBag.getInheritancePattern());

      assertEquals(2, hierarchicalBag.getDepth2entities().size());

      assertEquals(1, hierarchicalBag.getDepth2entities().get(0).size());
      Iterator<OEntity> it = hierarchicalBag.getDepth2entities().get(0).iterator();
      assertEquals("EMPLOYEE", it.next().getName());
      assertTrue(!it.hasNext());

      assertEquals(2, hierarchicalBag.getDepth2entities().get(1).size());
      it = hierarchicalBag.getDepth2entities().get(1).iterator();
      assertEquals("REGULAR_EMPLOYEE", it.next().getName());
      assertEquals("CONTRACT_EMPLOYEE", it.next().getName());
      assertTrue(!it.hasNext());

      assertEquals(hierarchicalBag, employeeEntity.getHierarchicalBag());
      assertEquals(hierarchicalBag, regularEmployeeEntity.getHierarchicalBag());
      assertEquals(hierarchicalBag, contractEmployeeEntity.getHierarchicalBag());

      assertNotNull(hierarchicalBag.getDiscriminatorColumn());
      assertEquals("employee_type",hierarchicalBag.getDiscriminatorColumn());


      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexByName("RegularEmployee");
      OVertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexByName("ContractEmployee");
      OVertexType residenceVertexType = mapper.getGraphModel().getVertexByName("Residence");


      // vertices check
      assertEquals(4, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);
      assertNotNull(residenceVertexType);

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

      assertNotNull(employeeVertexType.getPropertyByName("residence"));
      assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getPropertyType());
      assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());

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

      assertEquals(3, residenceVertexType.getProperties().size());

      assertNotNull(residenceVertexType.getPropertyByName("id"));
      assertEquals("id", residenceVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, residenceVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, residenceVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("city"));
      assertEquals("city", residenceVertexType.getPropertyByName("city").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("city").getPropertyType());
      assertEquals(2, residenceVertexType.getPropertyByName("city").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("city").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("country"));
      assertEquals("country", residenceVertexType.getPropertyByName("country").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("country").getPropertyType());
      assertEquals(3, residenceVertexType.getPropertyByName("country").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("country").isFromPrimaryKey());

      // inherited properties check
      assertEquals(0, employeeVertexType.getInheritedProperties().size());

      assertEquals(3, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getPropertyType());
      assertEquals(1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("name").getPropertyType());
      assertEquals(2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals("residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("residence").getPropertyType());
      assertEquals(3, regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(3, contractEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getPropertyType());
      assertEquals(1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("name").getPropertyType());
      assertEquals(2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals("residence", contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("residence").getPropertyType());
      assertEquals(3, contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(0, residenceVertexType.getInheritedProperties().size());

      // edges check

      assertEquals(1, mapper.getRelationship2edgeType().size());

      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertEquals("HasResidence", mapper.getGraphModel().getEdgesType().get(0).getName());

      assertEquals(1, employeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", employeeVertexType.getOutEdgesType().get(0).getName());

      assertEquals(1, regularEmployeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", regularEmployeeVertexType.getOutEdgesType().get(0).getName());

      assertEquals(1, contractEmployeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", contractEmployeeVertexType.getOutEdgesType().get(0).getName());

      // inheritance check
      assertEquals(employeeVertexType, regularEmployeeVertexType.getParentType());
      assertEquals(employeeVertexType, contractEmployeeVertexType.getParentType());
      assertNull(employeeVertexType.getParentType());

      assertEquals(1, regularEmployeeVertexType.getInheritanceLevel());
      assertEquals(1, contractEmployeeVertexType.getInheritanceLevel());
      assertEquals(0, employeeVertexType.getInheritanceLevel());



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

      String residence = "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding = "create memory table EMPLOYEE (ID varchar(256) not null,"+
          " NAME varchar(256), RESIDENCE varchar(256), primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding = "create memory table REGULAR_EMPLOYEE (ID varchar(256) not null, "
          + "NAME varchar(256), RESIDENCE varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), primary key (ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding = "create memory table CONTRACT_EMPLOYEE (ID varchar(256) not null, "
          + "NAME varchar(256), RESIDENCE varchar(256), PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper = new OHibernate2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", OHibernateMapperTestCase.XML_TABLE_PER_CONCRETE_CLASS, null, null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(4, context.getStatistics().totalNumberOfEntities);
      assertEquals(4, context.getStatistics().builtEntities);
      assertEquals(1, context.getStatistics().detectedRelationships);

      assertEquals(4, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(1, context.getStatistics().analizedRelationships);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema 
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity = mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");
      OEntity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");


      // entities check
      assertEquals(4, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(1, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(regularEmployeeEntity);
      assertNotNull(contractEmployeeEntity);
      assertNotNull(residenceEntity);


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

      assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(2, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(2, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals("PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals("DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals("CONTRACT_DURATION", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getBelongingEntity().getName());

      // inherited attributes check
      assertEquals(0, employeeEntity.getInheritedAttributes().size());

      assertEquals(3, regularEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(3, contractEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals("EMPLOYEE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getBelongingEntity().getName());

      // primary key check (not "inherited")
      assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals("ID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("VARCHAR", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals("REGULAR_EMPLOYEE", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());

      assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals("ID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("VARCHAR", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals("CONTRACT_EMPLOYEE", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getBelongingEntity().getName());


      // relationship, primary and foreign key check
      assertEquals(0, regularEmployeeEntity.getOutRelationships().size());
      assertEquals(0, contractEmployeeEntity.getOutRelationships().size());
      assertEquals(1, employeeEntity.getOutRelationships().size());
      assertEquals(0, residenceEntity.getOutRelationships().size());
      assertEquals(0, regularEmployeeEntity.getInRelationships().size());
      assertEquals(0, contractEmployeeEntity.getInRelationships().size());
      assertEquals(0, employeeEntity.getInRelationships().size());
      assertEquals(1, residenceEntity.getInRelationships().size());      

      assertEquals(0, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(0, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(0, residenceEntity.getForeignKeys().size());

      Iterator<ORelationship> itEmp = employeeEntity.getOutRelationships().iterator();
      ORelationship currentEmpRel = itEmp.next();
      assertEquals("RESIDENCE", currentEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentEmpRel.getForeignEntityName());
      assertEquals(residenceEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
      assertFalse(itEmp.hasNext());

      Iterator<ORelationship> itRes = residenceEntity.getInRelationships().iterator();
      ORelationship currentResRel = itRes.next();
      assertEquals(currentEmpRel, currentResRel);


      // inherited relationships check
      assertEquals(1, regularEmployeeEntity.getInheritedOutRelationships().size());
      assertEquals(1, contractEmployeeEntity.getInheritedOutRelationships().size());
      assertEquals(0, employeeEntity.getInheritedOutRelationships().size());
      assertEquals(0, residenceEntity.getInheritedOutRelationships().size());

      Iterator<ORelationship> itRegEmp = regularEmployeeEntity.getInheritedOutRelationships().iterator();
      Iterator<ORelationship> itContEmp = contractEmployeeEntity.getInheritedOutRelationships().iterator();
      ORelationship currentRegEmpRel = itRegEmp.next();
      ORelationship currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentRegEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntityName());
      assertEquals("RESIDENCE", currentContEmpRel.getParentEntityName());
      assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntityName());
      assertEquals(residenceEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(1, currentRegEmpRel.getForeignKey().getInvolvedAttributes().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getForeignKey().getInvolvedAttributes().get(0).getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(1, currentContEmpRel.getForeignKey().getInvolvedAttributes().size());
      assertEquals("RESIDENCE", currentContEmpRel.getForeignKey().getInvolvedAttributes().get(0).getName());
      assertFalse(itRegEmp.hasNext());
      assertFalse(itContEmp.hasNext());

      // inheritance check
      assertEquals(employeeEntity, regularEmployeeEntity.getParentEntity());
      assertEquals(employeeEntity, contractEmployeeEntity.getParentEntity());
      assertNull(employeeEntity.getParentEntity());
      assertNull(residenceEntity.getParentEntity());

      assertEquals(1, regularEmployeeEntity.getInheritanceLevel());
      assertEquals(1, contractEmployeeEntity.getInheritanceLevel());
      assertEquals(0, employeeEntity.getInheritanceLevel());
      assertEquals(0, residenceEntity.getInheritanceLevel());

      // Hierarchical Bag check
      assertEquals(1, mapper.getDataBaseSchema().getHierarchicalBags().size());
      OHierarchicalBag hierarchicalBag = mapper.getDataBaseSchema().getHierarchicalBags().get(0);
      assertEquals("table-per-concrete-type", hierarchicalBag.getInheritancePattern());

      assertEquals(2, hierarchicalBag.getDepth2entities().size());

      assertEquals(1, hierarchicalBag.getDepth2entities().get(0).size());
      Iterator<OEntity> it = hierarchicalBag.getDepth2entities().get(0).iterator();
      assertEquals("EMPLOYEE", it.next().getName());
      assertTrue(!it.hasNext());

      assertEquals(2, hierarchicalBag.getDepth2entities().get(1).size());
      it = hierarchicalBag.getDepth2entities().get(1).iterator();
      assertEquals("REGULAR_EMPLOYEE", it.next().getName());
      assertEquals("CONTRACT_EMPLOYEE", it.next().getName());
      assertTrue(!it.hasNext());

      assertEquals(hierarchicalBag, employeeEntity.getHierarchicalBag());
      assertEquals(hierarchicalBag, regularEmployeeEntity.getHierarchicalBag());
      assertEquals(hierarchicalBag, contractEmployeeEntity.getHierarchicalBag());

      assertNull(hierarchicalBag.getDiscriminatorColumn());


      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType regularEmployeeVertexType = mapper.getGraphModel().getVertexByName("RegularEmployee");
      OVertexType contractEmployeeVertexType = mapper.getGraphModel().getVertexByName("ContractEmployee");
      OVertexType residenceVertexType = mapper.getGraphModel().getVertexByName("Residence");


      // vertices check
      assertEquals(4, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);
      assertNotNull(residenceVertexType);

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

      assertNotNull(employeeVertexType.getPropertyByName("residence"));
      assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getPropertyType());
      assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());

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

      assertEquals(3, residenceVertexType.getProperties().size());

      assertNotNull(residenceVertexType.getPropertyByName("id"));
      assertEquals("id", residenceVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, residenceVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, residenceVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("city"));
      assertEquals("city", residenceVertexType.getPropertyByName("city").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("city").getPropertyType());
      assertEquals(2, residenceVertexType.getPropertyByName("city").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("city").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("country"));
      assertEquals("country", residenceVertexType.getPropertyByName("country").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("country").getPropertyType());
      assertEquals(3, residenceVertexType.getPropertyByName("country").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("country").isFromPrimaryKey());

      // inherited properties check
      assertEquals(0, employeeVertexType.getInheritedProperties().size());

      assertEquals(3, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getPropertyType());
      assertEquals(1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(true, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("name").getPropertyType());
      assertEquals(2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals("residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals("VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("residence").getPropertyType());
      assertEquals(3, regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(3, contractEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getPropertyType());
      assertEquals(1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(true, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("name").getPropertyType());
      assertEquals(2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals("residence", contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals("VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("residence").getPropertyType());
      assertEquals(3, contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(0, residenceVertexType.getInheritedProperties().size());

      // edges check

      assertEquals(1, mapper.getRelationship2edgeType().size());

      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertEquals("HasResidence", mapper.getGraphModel().getEdgesType().get(0).getName());

      assertEquals(1, employeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", employeeVertexType.getOutEdgesType().get(0).getName());

      assertEquals(1, regularEmployeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", regularEmployeeVertexType.getOutEdgesType().get(0).getName());

      assertEquals(1, contractEmployeeVertexType.getOutEdgesType().size());
      assertEquals("HasResidence", contractEmployeeVertexType.getOutEdgesType().get(0).getName());

      // inheritance check
      assertEquals(employeeVertexType, regularEmployeeVertexType.getParentType());
      assertEquals(employeeVertexType, contractEmployeeVertexType.getParentType());
      assertNull(employeeVertexType.getParentType());

      assertEquals(1, regularEmployeeVertexType.getInheritanceLevel());
      assertEquals(1, contractEmployeeVertexType.getInheritanceLevel());
      assertEquals(0, employeeVertexType.getInheritanceLevel());


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
