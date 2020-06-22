/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.teleporter.test.rdbms.inheritance.hibernate;

import static org.junit.Assert.*;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.OHibernate2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OCanonicalRelationship;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Before;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class HibernateMapperTest {

  private OER2GraphMapper mapper;
  private OTeleporterContext context;
  private ODBQueryEngine dbQueryEngine;
  private String driver = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private OSourceDatabaseInfo sourceDBInfo;
  private static final String XML_TABLE_PER_CLASS =
      "src/test/resources/inheritance/hibernate/tablePerClassHierarchyInheritanceTest.xml";
  private static final String XML_TABLE_PER_SUBCLASS1 =
      "src/test/resources/inheritance/hibernate/tablePerSubclassInheritanceTest1.xml";
  private static final String XML_TABLE_PER_SUBCLASS2 =
      "src/test/resources/inheritance/hibernate/tablePerSubclassInheritanceTest2.xml";
  private static final String XML_TABLE_PER_CONCRETE_CLASS =
      "src/test/resources/inheritance/hibernate/tablePerConcreteClassInheritanceTest.xml";
  private String outParentDirectory = "embedded:target/";

  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance(this.outParentDirectory);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setMessageHandler(new OTeleporterMessageHandler(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.sourceDBInfo =
        new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
  }

  // @Test

  /*
   * Table per Class Hierarchy Inheritance (<subclass> tag)
   *  table ( http://www.javatpoint.com/hibernate-table-per-hierarchy-example-using-xml-file )
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residence =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null,"
              + " TYPE varchar(256), NAME varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), "
              + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), RESIDENCE varchar(256),"
              + "primary key (id), foreign key (RESIDENCE) references RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      this.mapper =
          new OHibernate2GraphMapper(
              this.sourceDBInfo, HibernateMapperTest.XML_TABLE_PER_CLASS, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());

      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);
      assertEquals(1, context.getStatistics().totalNumberOfRelationships);
      assertEquals(1, context.getStatistics().builtRelationships);

      assertEquals(4, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(4, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByNameIgnoreCase("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByNameIgnoreCase("CONTRACT_EMPLOYEE");
      OEntity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      Assert.assertEquals(4, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
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
      assertEquals(
          "EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("NAME"));
      assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(2, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals(
          "Regular_Employee",
          regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals(
          "Regular_Employee",
          regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(2, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals(
          "PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals(
          "DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(
          1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals(
          "Contract_Employee",
          contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals(
          "CONTRACT_DURATION",
          contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(
          2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals(
          "Contract_Employee",
          contractEmployeeEntity
              .getAttributeByName("CONTRACT_DURATION")
              .getBelongingEntity()
              .getName());

      // inherited attributes check
      assertEquals(0, employeeEntity.getInheritedAttributes().size());

      assertEquals(3, regularEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(
          2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals(
          "RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(
          3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity
              .getInheritedAttributeByName("RESIDENCE")
              .getBelongingEntity()
              .getName());

      assertEquals(3, contractEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(
          1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(
          2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity
              .getInheritedAttributeByName("NAME")
              .getBelongingEntity()
              .getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals(
          "RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(
          3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity
              .getInheritedAttributeByName("RESIDENCE")
              .getBelongingEntity()
              .getName());

      // primary key check
      assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals(
          "ID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity
              .getPrimaryKey()
              .getInvolvedAttributes()
              .get(0)
              .getBelongingEntity()
              .getName());

      assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals(
          "ID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity
              .getPrimaryKey()
              .getInvolvedAttributes()
              .get(0)
              .getBelongingEntity()
              .getName());

      // relationship, primary and foreign key check
      assertEquals(0, regularEmployeeEntity.getOutCanonicalRelationships().size());
      assertEquals(0, contractEmployeeEntity.getOutCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
      assertEquals(0, residenceEntity.getOutCanonicalRelationships().size());
      assertEquals(0, regularEmployeeEntity.getInCanonicalRelationships().size());
      assertEquals(0, contractEmployeeEntity.getInCanonicalRelationships().size());
      assertEquals(0, employeeEntity.getInCanonicalRelationships().size());
      assertEquals(1, residenceEntity.getInCanonicalRelationships().size());

      assertEquals(0, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(0, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(0, residenceEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentEmpRel = itEmp.next();
      assertEquals("RESIDENCE", currentEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
      assertFalse(itEmp.hasNext());

      Iterator<OCanonicalRelationship> itRes =
          residenceEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentResRel = itRes.next();
      assertEquals(currentEmpRel, currentResRel);

      // inherited relationships check
      assertEquals(1, regularEmployeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(1, contractEmployeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(0, employeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(0, residenceEntity.getInheritedOutCanonicalRelationships().size());

      Iterator<OCanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      Iterator<OCanonicalRelationship> itContEmp =
          contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRegEmpRel = itRegEmp.next();
      OCanonicalRelationship currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentRegEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
      assertEquals("RESIDENCE", currentContEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(1, currentRegEmpRel.getFromColumns().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getFromColumns().get(0).getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(1, currentContEmpRel.getFromColumns().size());
      assertEquals("RESIDENCE", currentContEmpRel.getFromColumns().get(0).getName());
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
      Assert.assertEquals(1, mapper.getDataBaseSchema().getHierarchicalBags().size());
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
      assertEquals("TYPE", hierarchicalBag.getDiscriminatorColumn());

      assertEquals(3, hierarchicalBag.getEntityName2discriminatorValue().size());
      assertEquals("emp", hierarchicalBag.getEntityName2discriminatorValue().get("EMPLOYEE"));
      assertEquals(
          "reg_emp", hierarchicalBag.getEntityName2discriminatorValue().get("Regular_Employee"));
      assertEquals(
          "cont_emp", hierarchicalBag.getEntityName2discriminatorValue().get("Contract_Employee"));

      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      OVertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      OVertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      Assert.assertEquals(4, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);
      assertNotNull(residenceVertexType);

      // properties check
      assertEquals(3, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("id"));
      assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("residence"));
      assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
      assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
      assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
      assertEquals(
          "DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getOriginalType());
      assertEquals(1, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
      assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
      assertEquals(
          "DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType());
      assertEquals(2, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

      assertEquals(2, contractEmployeeVertexType.getProperties().size());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
      assertEquals(
          "payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
      assertEquals(
          "DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType());
      assertEquals(
          1, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
      assertEquals(
          "contractDuration",
          contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType());
      assertEquals(
          2, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
      assertEquals(
          false,
          contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

      assertEquals(3, residenceVertexType.getProperties().size());

      assertNotNull(residenceVertexType.getPropertyByName("id"));
      assertEquals("id", residenceVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, residenceVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, residenceVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("city"));
      assertEquals("city", residenceVertexType.getPropertyByName("city").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("city").getOriginalType());
      assertEquals(2, residenceVertexType.getPropertyByName("city").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("city").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("country"));
      assertEquals("country", residenceVertexType.getPropertyByName("country").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("country").getOriginalType());
      assertEquals(3, residenceVertexType.getPropertyByName("country").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("country").isFromPrimaryKey());

      // inherited properties check
      assertEquals(0, employeeVertexType.getInheritedProperties().size());

      assertEquals(3, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals(
          "VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
      assertEquals(
          1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(
          true, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
      assertEquals(
          2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(
          false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals(
          "residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
      assertEquals(
          3,
          regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(
          false,
          regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(3, contractEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals(
          "VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
      assertEquals(
          1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(
          true, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
      assertEquals(
          2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals(
          "residence",
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
      assertEquals(
          3,
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(
          false,
          contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(0, residenceVertexType.getInheritedProperties().size());

      // edges check

      assertEquals(1, mapper.getRelationship2edgeType().size());

      Assert.assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      Assert.assertEquals("HasResidence", mapper.getGraphModel().getEdgesType().get(0).getName());

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

  // @Test

  /*
   * Table per Subclass Inheritance (<joined-subclass> tag)
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-subclass )
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residence =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null,"
              + " NAME varchar(256), RESIDENCE varchar(256), primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding =
          "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
              + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding =
          "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
              + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper =
          new OHibernate2GraphMapper(
              this.sourceDBInfo, HibernateMapperTest.XML_TABLE_PER_SUBCLASS1, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());

      /*
       *  Testing context information
       */

      assertEquals(4, context.getStatistics().totalNumberOfEntities);
      assertEquals(4, context.getStatistics().builtEntities);
      assertEquals(3, context.getStatistics().totalNumberOfRelationships);
      assertEquals(3, context.getStatistics().builtRelationships);

      assertEquals(4, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(4, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");
      OEntity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      Assert.assertEquals(4, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(3, mapper.getDataBaseSchema().getCanonicalRelationships().size());
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
      assertEquals(
          "EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("NAME"));
      assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(2, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals(
          "REGULAR_EMPLOYEE",
          regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals(
          "REGULAR_EMPLOYEE",
          regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(2, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals(
          "PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals(
          "DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(
          1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals(
          "CONTRACT_EMPLOYEE",
          contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals(
          "CONTRACT_DURATION",
          contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(
          2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals(
          "CONTRACT_EMPLOYEE",
          contractEmployeeEntity
              .getAttributeByName("CONTRACT_DURATION")
              .getBelongingEntity()
              .getName());

      // inherited attributes check
      assertEquals(0, employeeEntity.getInheritedAttributes().size());

      assertEquals(3, regularEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(
          2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals(
          "RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(
          3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity
              .getInheritedAttributeByName("RESIDENCE")
              .getBelongingEntity()
              .getName());

      assertEquals(3, contractEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(
          1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(
          2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity
              .getInheritedAttributeByName("NAME")
              .getBelongingEntity()
              .getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals(
          "RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(
          3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity
              .getInheritedAttributeByName("RESIDENCE")
              .getBelongingEntity()
              .getName());

      // primary key check
      assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals(
          "EID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals(
          "REGULAR_EMPLOYEE",
          regularEmployeeEntity
              .getPrimaryKey()
              .getInvolvedAttributes()
              .get(0)
              .getBelongingEntity()
              .getName());

      assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals(
          "EID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals(
          "CONTRACT_EMPLOYEE",
          contractEmployeeEntity
              .getPrimaryKey()
              .getInvolvedAttributes()
              .get(0)
              .getBelongingEntity()
              .getName());

      // relationship, primary and foreign key check
      assertEquals(1, regularEmployeeEntity.getOutCanonicalRelationships().size());
      assertEquals(1, contractEmployeeEntity.getOutCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
      assertEquals(0, residenceEntity.getOutCanonicalRelationships().size());
      assertEquals(0, regularEmployeeEntity.getInCanonicalRelationships().size());
      assertEquals(0, contractEmployeeEntity.getInCanonicalRelationships().size());
      assertEquals(2, employeeEntity.getInCanonicalRelationships().size());
      assertEquals(1, residenceEntity.getInCanonicalRelationships().size());

      assertEquals(1, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(1, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(0, residenceEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      Iterator<OCanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getOutCanonicalRelationships().iterator();
      Iterator<OCanonicalRelationship> itContEmp =
          contractEmployeeEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentEmpRel = itEmp.next();
      OCanonicalRelationship currentRegEmpRel = itRegEmp.next();
      OCanonicalRelationship currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getParentEntity().getName());
      assertEquals("REGULAR_EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
      assertEquals("EMPLOYEE", currentContEmpRel.getParentEntity().getName());
      assertEquals("CONTRACT_EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(regularEmployeeEntity.getForeignKeys().get(0), currentRegEmpRel.getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(
          contractEmployeeEntity.getForeignKeys().get(0), currentContEmpRel.getForeignKey());
      assertFalse(itEmp.hasNext());
      assertFalse(itRegEmp.hasNext());
      assertFalse(itContEmp.hasNext());

      Iterator<OCanonicalRelationship> itRes =
          residenceEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentResRel = itRes.next();
      assertEquals(currentEmpRel, currentResRel);

      itEmp = employeeEntity.getInCanonicalRelationships().iterator();
      currentEmpRel = itEmp.next();
      assertEquals(currentEmpRel, currentContEmpRel);

      currentEmpRel = itEmp.next();
      assertEquals(currentEmpRel, currentRegEmpRel);

      // inherited relationships check
      assertEquals(1, regularEmployeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(1, contractEmployeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(0, employeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(0, residenceEntity.getInheritedOutCanonicalRelationships().size());

      itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      currentRegEmpRel = itRegEmp.next();
      currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentRegEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
      assertEquals("RESIDENCE", currentContEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(1, currentRegEmpRel.getFromColumns().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getFromColumns().get(0).getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(1, currentContEmpRel.getFromColumns().size());
      assertEquals("RESIDENCE", currentContEmpRel.getFromColumns().get(0).getName());
      assertFalse(itRegEmp.hasNext());
      assertFalse(itContEmp.hasNext());

      assertEquals(1, currentRegEmpRel.getFromColumns().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getFromColumns().get(0).getName());

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
      Assert.assertEquals(1, mapper.getDataBaseSchema().getHierarchicalBags().size());
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

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      OVertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      OVertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      Assert.assertEquals(4, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);
      assertNotNull(residenceVertexType);

      // properties check
      assertEquals(3, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("id"));
      assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("residence"));
      assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
      assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
      assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
      assertEquals(
          "DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getOriginalType());
      assertEquals(1, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
      assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
      assertEquals(
          "DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType());
      assertEquals(2, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

      assertEquals(2, contractEmployeeVertexType.getProperties().size());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
      assertEquals(
          "payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
      assertEquals(
          "DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType());
      assertEquals(
          1, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
      assertEquals(
          "contractDuration",
          contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType());
      assertEquals(
          2, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
      assertEquals(
          false,
          contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

      assertEquals(3, residenceVertexType.getProperties().size());

      assertNotNull(residenceVertexType.getPropertyByName("id"));
      assertEquals("id", residenceVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, residenceVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, residenceVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("city"));
      assertEquals("city", residenceVertexType.getPropertyByName("city").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("city").getOriginalType());
      assertEquals(2, residenceVertexType.getPropertyByName("city").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("city").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("country"));
      assertEquals("country", residenceVertexType.getPropertyByName("country").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("country").getOriginalType());
      assertEquals(3, residenceVertexType.getPropertyByName("country").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("country").isFromPrimaryKey());

      // inherited properties check
      assertEquals(0, employeeVertexType.getInheritedProperties().size());

      assertEquals(3, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals(
          "VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
      assertEquals(
          1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(
          false, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
      assertEquals(
          2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(
          false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals(
          "residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
      assertEquals(
          3,
          regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(
          false,
          regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(3, contractEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals(
          "VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
      assertEquals(
          1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
      assertEquals(
          2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals(
          "residence",
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
      assertEquals(
          3,
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(
          false,
          contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(0, residenceVertexType.getInheritedProperties().size());

      // edges check

      assertEquals(1, mapper.getRelationship2edgeType().size());

      Assert.assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      Assert.assertEquals("HasResidence", mapper.getGraphModel().getEdgesType().get(0).getName());

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

  // @Test

  /*
   * Table per Subclass Inheritance (<subclass> <join/> </subclass> tags)
   * 3 tables, one parent and 2 children ( http://www.javatpoint.com/table-per-subclass )
   */

  public void test3() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residence =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null,"
              + " NAME varchar(256), RESIDENCE varchar(256), primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding =
          "create memory table REGULAR_EMPLOYEE (EID varchar(256) not null, "
              + "SALARY decimal(10,2), BONUS decimal(10,0), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding =
          "create memory table CONTRACT_EMPLOYEE (EID varchar(256) not null, "
              + "PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (EID), foreign key (EID) references EMPLOYEE(ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper =
          new OHibernate2GraphMapper(
              this.sourceDBInfo, HibernateMapperTest.XML_TABLE_PER_SUBCLASS2, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());

      /*
       *  Testing context information
       */

      assertEquals(4, context.getStatistics().totalNumberOfEntities);
      assertEquals(4, context.getStatistics().builtEntities);
      assertEquals(3, context.getStatistics().totalNumberOfRelationships);
      assertEquals(3, context.getStatistics().builtRelationships);

      assertEquals(4, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(4, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");
      OEntity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      Assert.assertEquals(4, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(3, mapper.getDataBaseSchema().getCanonicalRelationships().size());
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
      assertEquals(
          "EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("NAME"));
      assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(2, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals(
          "REGULAR_EMPLOYEE",
          regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals(
          "REGULAR_EMPLOYEE",
          regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(2, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals(
          "PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals(
          "DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(
          1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals(
          "CONTRACT_EMPLOYEE",
          contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals(
          "CONTRACT_DURATION",
          contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(
          2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals(
          "CONTRACT_EMPLOYEE",
          contractEmployeeEntity
              .getAttributeByName("CONTRACT_DURATION")
              .getBelongingEntity()
              .getName());

      // inherited attributes check
      assertEquals(0, employeeEntity.getInheritedAttributes().size());

      assertEquals(3, regularEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(
          2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals(
          "RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(
          3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity
              .getInheritedAttributeByName("RESIDENCE")
              .getBelongingEntity()
              .getName());

      assertEquals(3, contractEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(
          1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(
          2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity
              .getInheritedAttributeByName("NAME")
              .getBelongingEntity()
              .getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals(
          "RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(
          3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity
              .getInheritedAttributeByName("RESIDENCE")
              .getBelongingEntity()
              .getName());

      // primary key check
      assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals(
          "EID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals(
          "REGULAR_EMPLOYEE",
          regularEmployeeEntity
              .getPrimaryKey()
              .getInvolvedAttributes()
              .get(0)
              .getBelongingEntity()
              .getName());

      assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals(
          "EID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals(
          "CONTRACT_EMPLOYEE",
          contractEmployeeEntity
              .getPrimaryKey()
              .getInvolvedAttributes()
              .get(0)
              .getBelongingEntity()
              .getName());

      // relationship, primary and foreign key check
      assertEquals(1, regularEmployeeEntity.getOutCanonicalRelationships().size());
      assertEquals(1, contractEmployeeEntity.getOutCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
      assertEquals(0, residenceEntity.getOutCanonicalRelationships().size());
      assertEquals(0, regularEmployeeEntity.getInCanonicalRelationships().size());
      assertEquals(0, contractEmployeeEntity.getInCanonicalRelationships().size());
      assertEquals(2, employeeEntity.getInCanonicalRelationships().size());
      assertEquals(1, residenceEntity.getInCanonicalRelationships().size());

      assertEquals(1, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(1, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(0, residenceEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      Iterator<OCanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getOutCanonicalRelationships().iterator();
      Iterator<OCanonicalRelationship> itContEmp =
          contractEmployeeEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentEmpRel = itEmp.next();
      OCanonicalRelationship currentRegEmpRel = itRegEmp.next();
      OCanonicalRelationship currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getParentEntity().getName());
      assertEquals("REGULAR_EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
      assertEquals("EMPLOYEE", currentContEmpRel.getParentEntity().getName());
      assertEquals("CONTRACT_EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(regularEmployeeEntity.getForeignKeys().get(0), currentRegEmpRel.getForeignKey());
      assertEquals(employeeEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(
          contractEmployeeEntity.getForeignKeys().get(0), currentContEmpRel.getForeignKey());
      assertFalse(itEmp.hasNext());
      assertFalse(itRegEmp.hasNext());
      assertFalse(itContEmp.hasNext());

      Iterator<OCanonicalRelationship> itRes =
          residenceEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentResRel = itRes.next();
      assertEquals(currentEmpRel, currentResRel);

      itEmp = employeeEntity.getInCanonicalRelationships().iterator();
      currentEmpRel = itEmp.next();
      assertEquals(currentEmpRel, currentContEmpRel);

      currentEmpRel = itEmp.next();
      assertEquals(currentEmpRel, currentRegEmpRel);

      // inherited relationships check
      assertEquals(1, regularEmployeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(1, contractEmployeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(0, employeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(0, residenceEntity.getInheritedOutCanonicalRelationships().size());

      itRegEmp = regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      itContEmp = contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      currentRegEmpRel = itRegEmp.next();
      currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentRegEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
      assertEquals("RESIDENCE", currentContEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(1, currentRegEmpRel.getFromColumns().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getFromColumns().get(0).getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(1, currentContEmpRel.getFromColumns().size());
      assertEquals("RESIDENCE", currentContEmpRel.getFromColumns().get(0).getName());
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
      Assert.assertEquals(1, mapper.getDataBaseSchema().getHierarchicalBags().size());
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
      assertEquals("employee_type", hierarchicalBag.getDiscriminatorColumn());

      /*
       *  Testing built graph model
       */

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      OVertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      OVertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      Assert.assertEquals(4, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);
      assertNotNull(residenceVertexType);

      // properties check
      assertEquals(3, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("id"));
      assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("residence"));
      assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
      assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
      assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
      assertEquals(
          "DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getOriginalType());
      assertEquals(1, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
      assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
      assertEquals(
          "DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType());
      assertEquals(2, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

      assertEquals(2, contractEmployeeVertexType.getProperties().size());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
      assertEquals(
          "payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
      assertEquals(
          "DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType());
      assertEquals(
          1, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
      assertEquals(
          "contractDuration",
          contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType());
      assertEquals(
          2, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
      assertEquals(
          false,
          contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

      assertEquals(3, residenceVertexType.getProperties().size());

      assertNotNull(residenceVertexType.getPropertyByName("id"));
      assertEquals("id", residenceVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, residenceVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, residenceVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("city"));
      assertEquals("city", residenceVertexType.getPropertyByName("city").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("city").getOriginalType());
      assertEquals(2, residenceVertexType.getPropertyByName("city").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("city").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("country"));
      assertEquals("country", residenceVertexType.getPropertyByName("country").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("country").getOriginalType());
      assertEquals(3, residenceVertexType.getPropertyByName("country").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("country").isFromPrimaryKey());

      // inherited properties check
      assertEquals(0, employeeVertexType.getInheritedProperties().size());

      assertEquals(3, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals(
          "VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
      assertEquals(
          1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(
          false, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
      assertEquals(
          2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(
          false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals(
          "residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
      assertEquals(
          3,
          regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(
          false,
          regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(3, contractEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals(
          "VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
      assertEquals(
          1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
      assertEquals(
          2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals(
          "residence",
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
      assertEquals(
          3,
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(
          false,
          contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(0, residenceVertexType.getInheritedProperties().size());

      // edges check

      assertEquals(1, mapper.getRelationship2edgeType().size());

      Assert.assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      Assert.assertEquals("HasResidence", mapper.getGraphModel().getEdgesType().get(0).getName());

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

  // @Test

  /*
   * Table per Concrete Class Inheritance (<union-subclass> tag)
   * 3 tables, one parent and 2 childs ( http://www.javatpoint.com/table-per-concrete-class )
   */

  public void test4() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String residence =
          "create memory table RESIDENCE(ID varchar(256) not null, CITY varchar(256), COUNTRY varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(residence);

      String employeeTableBuilding =
          "create memory table EMPLOYEE (ID varchar(256) not null,"
              + " NAME varchar(256), RESIDENCE varchar(256), primary key (ID), foreign key (RESIDENCE) references RESIDENCE(ID))";
      st.execute(employeeTableBuilding);

      String regularEmployeeTableBuilding =
          "create memory table REGULAR_EMPLOYEE (ID varchar(256) not null, "
              + "NAME varchar(256), RESIDENCE varchar(256), SALARY decimal(10,2), BONUS decimal(10,0), primary key (ID))";
      st.execute(regularEmployeeTableBuilding);

      String contractEmployeeTableBuilding =
          "create memory table CONTRACT_EMPLOYEE (ID varchar(256) not null, "
              + "NAME varchar(256), RESIDENCE varchar(256), PAY_PER_HOUR decimal(10,2), CONTRACT_DURATION varchar(256), primary key (ID))";
      st.execute(contractEmployeeTableBuilding);

      this.mapper =
          new OHibernate2GraphMapper(
              this.sourceDBInfo,
              HibernateMapperTest.XML_TABLE_PER_CONCRETE_CLASS,
              null,
              null,
              null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());

      /*
       *  Testing context information
       */

      assertEquals(4, context.getStatistics().totalNumberOfEntities);
      assertEquals(4, context.getStatistics().builtEntities);
      assertEquals(1, context.getStatistics().totalNumberOfRelationships);
      assertEquals(1, context.getStatistics().builtRelationships);

      assertEquals(4, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(4, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);

      /*
       *  Testing built source db schema
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity regularEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByName("REGULAR_EMPLOYEE");
      OEntity contractEmployeeEntity =
          mapper.getDataBaseSchema().getEntityByName("CONTRACT_EMPLOYEE");
      OEntity residenceEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase("RESIDENCE");

      // entities check
      Assert.assertEquals(4, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
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
      assertEquals(
          "EMPLOYEE", employeeEntity.getAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("NAME"));
      assertEquals("NAME", employeeEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("NAME").getDataType());
      assertEquals(2, employeeEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE", employeeEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(employeeEntity.getAttributeByName("RESIDENCE"));
      assertEquals("RESIDENCE", employeeEntity.getAttributeByName("RESIDENCE").getName());
      assertEquals("VARCHAR", employeeEntity.getAttributeByName("RESIDENCE").getDataType());
      assertEquals(3, employeeEntity.getAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          employeeEntity.getAttributeByName("RESIDENCE").getBelongingEntity().getName());

      assertEquals(2, regularEmployeeEntity.getAttributes().size());

      assertNotNull(regularEmployeeEntity.getAttributeByName("SALARY"));
      assertEquals("SALARY", regularEmployeeEntity.getAttributeByName("SALARY").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("SALARY").getDataType());
      assertEquals(1, regularEmployeeEntity.getAttributeByName("SALARY").getOrdinalPosition());
      assertEquals(
          "REGULAR_EMPLOYEE",
          regularEmployeeEntity.getAttributeByName("SALARY").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getAttributeByName("BONUS"));
      assertEquals("BONUS", regularEmployeeEntity.getAttributeByName("BONUS").getName());
      assertEquals("DECIMAL", regularEmployeeEntity.getAttributeByName("BONUS").getDataType());
      assertEquals(2, regularEmployeeEntity.getAttributeByName("BONUS").getOrdinalPosition());
      assertEquals(
          "REGULAR_EMPLOYEE",
          regularEmployeeEntity.getAttributeByName("BONUS").getBelongingEntity().getName());

      assertEquals(2, contractEmployeeEntity.getAttributes().size());

      assertNotNull(contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR"));
      assertEquals(
          "PAY_PER_HOUR", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getName());
      assertEquals(
          "DECIMAL", contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getDataType());
      assertEquals(
          1, contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getOrdinalPosition());
      assertEquals(
          "CONTRACT_EMPLOYEE",
          contractEmployeeEntity.getAttributeByName("PAY_PER_HOUR").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION"));
      assertEquals(
          "CONTRACT_DURATION",
          contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getDataType());
      assertEquals(
          2, contractEmployeeEntity.getAttributeByName("CONTRACT_DURATION").getOrdinalPosition());
      assertEquals(
          "CONTRACT_EMPLOYEE",
          contractEmployeeEntity
              .getAttributeByName("CONTRACT_DURATION")
              .getBelongingEntity()
              .getName());

      // inherited attributes check
      assertEquals(0, employeeEntity.getInheritedAttributes().size());

      assertEquals(3, regularEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", regularEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(1, regularEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", regularEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(
          2, regularEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity.getInheritedAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals(
          "RESIDENCE", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals(
          "VARCHAR", regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(
          3, regularEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          regularEmployeeEntity
              .getInheritedAttributeByName("RESIDENCE")
              .getBelongingEntity()
              .getName());

      assertEquals(3, contractEmployeeEntity.getInheritedAttributes().size());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("ID"));
      assertEquals("ID", contractEmployeeEntity.getInheritedAttributeByName("ID").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("ID").getDataType());
      assertEquals(
          1, contractEmployeeEntity.getInheritedAttributeByName("ID").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity.getInheritedAttributeByName("ID").getBelongingEntity().getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("NAME"));
      assertEquals("NAME", contractEmployeeEntity.getInheritedAttributeByName("NAME").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("NAME").getDataType());
      assertEquals(
          2, contractEmployeeEntity.getInheritedAttributeByName("NAME").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity
              .getInheritedAttributeByName("NAME")
              .getBelongingEntity()
              .getName());

      assertNotNull(contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE"));
      assertEquals(
          "RESIDENCE", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getName());
      assertEquals(
          "VARCHAR", contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getDataType());
      assertEquals(
          3, contractEmployeeEntity.getInheritedAttributeByName("RESIDENCE").getOrdinalPosition());
      assertEquals(
          "EMPLOYEE",
          contractEmployeeEntity
              .getInheritedAttributeByName("RESIDENCE")
              .getBelongingEntity()
              .getName());

      // primary key check (not "inherited")
      assertEquals(1, regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals(
          "ID", regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals(
          "REGULAR_EMPLOYEE",
          regularEmployeeEntity
              .getPrimaryKey()
              .getInvolvedAttributes()
              .get(0)
              .getBelongingEntity()
              .getName());

      assertEquals(1, contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().size());
      assertEquals(
          "ID", contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getDataType());
      assertEquals(
          "CONTRACT_EMPLOYEE",
          contractEmployeeEntity
              .getPrimaryKey()
              .getInvolvedAttributes()
              .get(0)
              .getBelongingEntity()
              .getName());

      // relationship, primary and foreign key check
      assertEquals(0, regularEmployeeEntity.getOutCanonicalRelationships().size());
      assertEquals(0, contractEmployeeEntity.getOutCanonicalRelationships().size());
      assertEquals(1, employeeEntity.getOutCanonicalRelationships().size());
      assertEquals(0, residenceEntity.getOutCanonicalRelationships().size());
      assertEquals(0, regularEmployeeEntity.getInCanonicalRelationships().size());
      assertEquals(0, contractEmployeeEntity.getInCanonicalRelationships().size());
      assertEquals(0, employeeEntity.getInCanonicalRelationships().size());
      assertEquals(1, residenceEntity.getInCanonicalRelationships().size());

      assertEquals(0, regularEmployeeEntity.getForeignKeys().size());
      assertEquals(0, contractEmployeeEntity.getForeignKeys().size());
      assertEquals(1, employeeEntity.getForeignKeys().size());
      assertEquals(0, residenceEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> itEmp =
          employeeEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentEmpRel = itEmp.next();
      assertEquals("RESIDENCE", currentEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentEmpRel.getForeignEntity().getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentEmpRel.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentEmpRel.getForeignKey());
      assertFalse(itEmp.hasNext());

      Iterator<OCanonicalRelationship> itRes =
          residenceEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentResRel = itRes.next();
      assertEquals(currentEmpRel, currentResRel);

      // inherited relationships check
      assertEquals(1, regularEmployeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(1, contractEmployeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(0, employeeEntity.getInheritedOutCanonicalRelationships().size());
      assertEquals(0, residenceEntity.getInheritedOutCanonicalRelationships().size());

      Iterator<OCanonicalRelationship> itRegEmp =
          regularEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      Iterator<OCanonicalRelationship> itContEmp =
          contractEmployeeEntity.getInheritedOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRegEmpRel = itRegEmp.next();
      OCanonicalRelationship currentContEmpRel = itContEmp.next();
      assertEquals("RESIDENCE", currentRegEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentRegEmpRel.getForeignEntity().getName());
      assertEquals("RESIDENCE", currentContEmpRel.getParentEntity().getName());
      assertEquals("EMPLOYEE", currentContEmpRel.getForeignEntity().getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentRegEmpRel.getPrimaryKey());
      assertEquals(1, currentRegEmpRel.getFromColumns().size());
      assertEquals("RESIDENCE", currentRegEmpRel.getFromColumns().get(0).getName());
      assertEquals(residenceEntity.getPrimaryKey(), currentContEmpRel.getPrimaryKey());
      assertEquals(1, currentContEmpRel.getFromColumns().size());
      assertEquals("RESIDENCE", currentContEmpRel.getFromColumns().get(0).getName());
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
      Assert.assertEquals(1, mapper.getDataBaseSchema().getHierarchicalBags().size());
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

      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType regularEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("RegularEmployee");
      OVertexType contractEmployeeVertexType =
          mapper.getGraphModel().getVertexTypeByName("ContractEmployee");
      OVertexType residenceVertexType = mapper.getGraphModel().getVertexTypeByName("Residence");

      // vertices check
      Assert.assertEquals(4, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(regularEmployeeVertexType);
      assertNotNull(contractEmployeeVertexType);
      assertNotNull(residenceVertexType);

      // properties check
      assertEquals(3, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("id"));
      assertEquals("id", employeeVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, employeeVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(2, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("residence"));
      assertEquals("residence", employeeVertexType.getPropertyByName("residence").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("residence").getOriginalType());
      assertEquals(3, employeeVertexType.getPropertyByName("residence").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("residence").isFromPrimaryKey());

      assertEquals(2, regularEmployeeVertexType.getProperties().size());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("salary"));
      assertEquals("salary", regularEmployeeVertexType.getPropertyByName("salary").getName());
      assertEquals(
          "DECIMAL", regularEmployeeVertexType.getPropertyByName("salary").getOriginalType());
      assertEquals(1, regularEmployeeVertexType.getPropertyByName("salary").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("salary").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getPropertyByName("bonus"));
      assertEquals("bonus", regularEmployeeVertexType.getPropertyByName("bonus").getName());
      assertEquals(
          "DECIMAL", regularEmployeeVertexType.getPropertyByName("bonus").getOriginalType());
      assertEquals(2, regularEmployeeVertexType.getPropertyByName("bonus").getOrdinalPosition());
      assertEquals(false, regularEmployeeVertexType.getPropertyByName("bonus").isFromPrimaryKey());

      assertEquals(2, contractEmployeeVertexType.getProperties().size());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("payPerHour"));
      assertEquals(
          "payPerHour", contractEmployeeVertexType.getPropertyByName("payPerHour").getName());
      assertEquals(
          "DECIMAL", contractEmployeeVertexType.getPropertyByName("payPerHour").getOriginalType());
      assertEquals(
          1, contractEmployeeVertexType.getPropertyByName("payPerHour").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getPropertyByName("payPerHour").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getPropertyByName("contractDuration"));
      assertEquals(
          "contractDuration",
          contractEmployeeVertexType.getPropertyByName("contractDuration").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getPropertyByName("contractDuration").getOriginalType());
      assertEquals(
          2, contractEmployeeVertexType.getPropertyByName("contractDuration").getOrdinalPosition());
      assertEquals(
          false,
          contractEmployeeVertexType.getPropertyByName("contractDuration").isFromPrimaryKey());

      assertEquals(3, residenceVertexType.getProperties().size());

      assertNotNull(residenceVertexType.getPropertyByName("id"));
      assertEquals("id", residenceVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, residenceVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, residenceVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("city"));
      assertEquals("city", residenceVertexType.getPropertyByName("city").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("city").getOriginalType());
      assertEquals(2, residenceVertexType.getPropertyByName("city").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("city").isFromPrimaryKey());

      assertNotNull(residenceVertexType.getPropertyByName("country"));
      assertEquals("country", residenceVertexType.getPropertyByName("country").getName());
      assertEquals("VARCHAR", residenceVertexType.getPropertyByName("country").getOriginalType());
      assertEquals(3, residenceVertexType.getPropertyByName("country").getOrdinalPosition());
      assertEquals(false, residenceVertexType.getPropertyByName("country").isFromPrimaryKey());

      // inherited properties check
      assertEquals(0, employeeVertexType.getInheritedProperties().size());

      assertEquals(3, regularEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", regularEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals(
          "VARCHAR", regularEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
      assertEquals(
          1, regularEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(
          true, regularEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", regularEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
      assertEquals(
          2, regularEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(
          false, regularEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(regularEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals(
          "residence", regularEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals(
          "VARCHAR",
          regularEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
      assertEquals(
          3,
          regularEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(
          false,
          regularEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(3, contractEmployeeVertexType.getInheritedProperties().size());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("id"));
      assertEquals("id", contractEmployeeVertexType.getInheritedPropertyByName("id").getName());
      assertEquals(
          "VARCHAR", contractEmployeeVertexType.getInheritedPropertyByName("id").getOriginalType());
      assertEquals(
          1, contractEmployeeVertexType.getInheritedPropertyByName("id").getOrdinalPosition());
      assertEquals(
          true, contractEmployeeVertexType.getInheritedPropertyByName("id").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("name"));
      assertEquals("name", contractEmployeeVertexType.getInheritedPropertyByName("name").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getInheritedPropertyByName("name").getOriginalType());
      assertEquals(
          2, contractEmployeeVertexType.getInheritedPropertyByName("name").getOrdinalPosition());
      assertEquals(
          false, contractEmployeeVertexType.getInheritedPropertyByName("name").isFromPrimaryKey());

      assertNotNull(contractEmployeeVertexType.getInheritedPropertyByName("residence"));
      assertEquals(
          "residence",
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getName());
      assertEquals(
          "VARCHAR",
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getOriginalType());
      assertEquals(
          3,
          contractEmployeeVertexType.getInheritedPropertyByName("residence").getOrdinalPosition());
      assertEquals(
          false,
          contractEmployeeVertexType.getInheritedPropertyByName("residence").isFromPrimaryKey());

      assertEquals(0, residenceVertexType.getInheritedProperties().size());

      // edges check

      assertEquals(1, mapper.getRelationship2edgeType().size());

      Assert.assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      Assert.assertEquals("HasResidence", mapper.getGraphModel().getEdgesType().get(0).getName());

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
