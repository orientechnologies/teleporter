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

package com.orientdb.teleporter.test.rdbms.mapper;

import com.orientdb.teleporter.context.OOutputStreamManager;
import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientdb.teleporter.model.dbschema.OEntity;
import com.orientdb.teleporter.model.dbschema.ORelationship;
import com.orientdb.teleporter.model.graphmodel.OEdgeType;
import com.orientdb.teleporter.model.graphmodel.OVertexType;
import com.orientdb.teleporter.nameresolver.OJavaConventionNameResolver;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OConfigurationMapping {

  private OER2GraphMapper    mapper;
  private  OTeleporterContext context;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    context.setOutputManager(new OOutputStreamManager(0));
    this.context.setQueryQuoteType("\"");
  }

  @Test

  /*
   *  Two tables: 2 relationships not declared through foreign keys.
   *  EMPLOYEE --[AssignedTo]--> PROJECT
   *  PROJECT --[HasManager]--> EMPLOYEE
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String parentTableBuilding = "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"+
          " FIRST_NAME varchar(256) not null, LAST_NAME varchar(256) not null, primary key (EMP_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table PROJECT (ID  varchar(256),"+
          " TITLE varchar(256) not null, PROJECT_MANAGER varchar(256) not null, primary key (ID))";
      st.execute(foreignTableBuilding);

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().analizedRelationships);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);

       /*
       *  Testing built source db schema
       */

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      OEntity projectEntity = mapper.getDataBaseSchema().getEntityByName("PROJECT");

      // entities check
      assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(2, mapper.getDataBaseSchema().getRelationships().size());
      assertNotNull(employeeEntity);
      assertNotNull(projectEntity);

      // attributes check
      assertEquals(3, employeeEntity.getAttributes().size());

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
      assertEquals(2, mapper.getDataBaseSchema().getRelationships().size());
      assertEquals(1, projectEntity.getOutRelationships().size());
      assertEquals(1, employeeEntity.getOutRelationships().size());
      assertEquals(1, projectEntity.getInRelationships().size());
      assertEquals(1, employeeEntity.getInRelationships().size());
      assertEquals(1,employeeEntity.getForeignKeys().size());
      assertEquals(1,projectEntity.getForeignKeys().size());

      Iterator<ORelationship> it = projectEntity.getOutRelationships().iterator();
      ORelationship currentRelationship = it.next();
      assertEquals("EMPLOYEE", currentRelationship.getParentEntityName());
      assertEquals("PROJECT", currentRelationship.getForeignEntityName());
      assertEquals(employeeEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(projectEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<ORelationship> it2 = employeeEntity.getInRelationships().iterator();
      ORelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT_MANAGER", projectEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("EMP_ID", employeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

      it = employeeEntity.getOutRelationships().iterator();
      currentRelationship = it.next();
      assertEquals("PROJECT", currentRelationship.getParentEntityName());
      assertEquals("EMPLOYEE", currentRelationship.getForeignEntityName());
      assertEquals(projectEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(employeeEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      it2 = employeeEntity.getInRelationships().iterator();
      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT_MANAGER", projectEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("EMP_ID", employeeEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());


      /*
       *  Testing built graph model
       */
      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType projectVertexType = mapper.getGraphModel().getVertexByName("Project");
      OEdgeType projectManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasProjectManager");
      OEdgeType mgrEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasMgr");


      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);

      // properties check
//      assertEquals(3, employeeVertexType.getProperties().size());
//
//      assertNotNull(employeeVertexType.getPropertyByName("empId"));
//      assertEquals("empId", employeeVertexType.getPropertyByName("empId").getName());
//      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("empId").getPropertyType());
//      assertEquals(1, employeeVertexType.getPropertyByName("empId").getOrdinalPosition());
//      assertEquals(true, employeeVertexType.getPropertyByName("empId").isFromPrimaryKey());
//
//      assertNotNull(employeeVertexType.getPropertyByName("mgrId"));
//      assertEquals("mgrId", employeeVertexType.getPropertyByName("mgrId").getName());
//      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("mgrId").getPropertyType());
//      assertEquals(2, employeeVertexType.getPropertyByName("mgrId").getOrdinalPosition());
//      assertEquals(false, employeeVertexType.getPropertyByName("mgrId").isFromPrimaryKey());
//
//      assertNotNull(employeeVertexType.getPropertyByName("name"));
//      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
//      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getPropertyType());
//      assertEquals(3, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
//      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());
//
//      assertEquals(3, projectVertexType.getProperties().size());
//
//      assertNotNull(projectVertexType.getPropertyByName("id"));
//      assertEquals("id", projectVertexType.getPropertyByName("id").getName());
//      assertEquals("VARCHAR", projectVertexType.getPropertyByName("id").getPropertyType());
//      assertEquals(1, projectVertexType.getPropertyByName("id").getOrdinalPosition());
//      assertEquals(true, projectVertexType.getPropertyByName("id").isFromPrimaryKey());
//
//      assertNotNull(projectVertexType.getPropertyByName("title"));
//      assertEquals("title", projectVertexType.getPropertyByName("title").getName());
//      assertEquals("VARCHAR", projectVertexType.getPropertyByName("title").getPropertyType());
//      assertEquals(2, projectVertexType.getPropertyByName("title").getOrdinalPosition());
//      assertEquals(false, projectVertexType.getPropertyByName("title").isFromPrimaryKey());
//
//      assertNotNull(projectVertexType.getPropertyByName("projectManager"));
//      assertEquals("projectManager", projectVertexType.getPropertyByName("projectManager").getName());
//      assertEquals("VARCHAR", projectVertexType.getPropertyByName("projectManager").getPropertyType());
//      assertEquals(3, projectVertexType.getPropertyByName("projectManager").getOrdinalPosition());
//      assertEquals(false, projectVertexType.getPropertyByName("projectManager").isFromPrimaryKey());

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(mgrEdgeType);
      assertNotNull(projectManagerEdgeType);

      assertEquals("HasManager", mgrEdgeType.getName());
      assertEquals(0, mgrEdgeType.getProperties().size());
      assertEquals("Employee", mgrEdgeType.getInVertexType().getName());
      assertEquals(1, mgrEdgeType.getNumberRelationshipsRepresented());

      assertEquals("AssignedTo", projectManagerEdgeType.getName());
      assertEquals(0, projectManagerEdgeType.getProperties().size());
      assertEquals("Project", projectManagerEdgeType.getInVertexType().getName());
      assertEquals(1, projectManagerEdgeType.getNumberRelationshipsRepresented());


    }catch(Exception e) {
      e.printStackTrace();
    }finally {
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test

  /*
   *  Two tables: 2 relationships declared through foreign keys but overridden with a configuration.
   *
   *  EMPLOYEE: foreign key (PROJECT) references PROJECT(ID)
   *  PROJECT: foreign key (PROJ_MGR) references EMPLOYEE(EMP_ID)
   *
   *  With default mapping we would have:
   *
   *  EMPLOYEE --[AssignedTo]--> PROJECT
   *  PROJECT --[HasManager]--> EMPLOYEE
   *
   *  But with configuration:
   *
   *  PROJECT --[HasEmployee]--> EMPLOYEE
   *  EMPLOYEE --[IsManagerFor]--> PROJECT
   *
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String parentTableBuilding = "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"+
          " NAME varchar(256) not null, primary key (EMP_ID), " +
          " foreign key (MGR_ID) references EMPLOYEE(EMP_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table PROJECT (ID  varchar(256),"+
          " TITLE varchar(256) not null, PROJECT_MANAGER varchar(256) not null, primary key (ID)," +
          " foreign key (PROJECT_MANAGER) references EMPLOYEE(EMP_ID))";
      st.execute(foreignTableBuilding);

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().analizedRelationships);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType employeeVertexType = mapper.getGraphModel().getVertexByName("Employee");
      OVertexType projectVertexType = mapper.getGraphModel().getVertexByName("Project");
      OEdgeType projectManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasProjectManager");
      OEdgeType mgrEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasMgr");


      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);

      // properties check
      assertEquals(3, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("empId"));
      assertEquals("empId", employeeVertexType.getPropertyByName("empId").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("empId").getPropertyType());
      assertEquals(1, employeeVertexType.getPropertyByName("empId").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("empId").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("mgrId"));
      assertEquals("mgrId", employeeVertexType.getPropertyByName("mgrId").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("mgrId").getPropertyType());
      assertEquals(2, employeeVertexType.getPropertyByName("mgrId").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("mgrId").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(3, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertEquals(3, projectVertexType.getProperties().size());

      assertNotNull(projectVertexType.getPropertyByName("id"));
      assertEquals("id", projectVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, projectVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, projectVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("title"));
      assertEquals("title", projectVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("title").getPropertyType());
      assertEquals(2, projectVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("projectManager"));
      assertEquals("projectManager", projectVertexType.getPropertyByName("projectManager").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("projectManager").getPropertyType());
      assertEquals(3, projectVertexType.getPropertyByName("projectManager").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("projectManager").isFromPrimaryKey());

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(mgrEdgeType);
      assertNotNull(projectManagerEdgeType);

      assertEquals("HasMgr", mgrEdgeType.getName());
      assertEquals(0, mgrEdgeType.getProperties().size());
      assertEquals("Employee", mgrEdgeType.getInVertexType().getName());
      assertEquals(1, mgrEdgeType.getNumberRelationshipsRepresented());

      assertEquals("HasProjectManager", projectManagerEdgeType.getName());
      assertEquals(0, projectManagerEdgeType.getProperties().size());
      assertEquals("Employee", projectManagerEdgeType.getInVertexType().getName());
      assertEquals(1, projectManagerEdgeType.getNumberRelationshipsRepresented());


    }catch(Exception e) {
      e.printStackTrace();
    }finally {
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test

  /*
   *  Three tables: 2 Parent and 1 join table which imports two different simple primary key.
   */

  public void test3() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null," +
          " TITLE varchar(256) not null, YEAR date, primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null,"+
          " NAME varchar(256) not null, SURNAME varchar(256) not null, primary key (ID))";
      st.execute(actorTableBuilding);

      String film2actorTableBuilding = "create memory table FILM_ACTOR (FILM_ID varchar(256) not null," +
          " ACTOR_ID varchar(256) not null, primary key (FILM_ID,ACTOR_ID)," +
          " foreign key (FILM_ID) references FILM(ID)," +
          " foreign key (ACTOR_ID) references ACTOR(ID))";
      st.execute(film2actorTableBuilding);

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().analizedRelationships);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType actorVertexType = mapper.getGraphModel().getVertexByName("Actor");
      OVertexType filmVertexType = mapper.getGraphModel().getVertexByName("Film");
      OVertexType film2actorVertexType = mapper.getGraphModel().getVertexByName("FilmActor");
      OEdgeType actorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasActor");
      OEdgeType filmEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasFilm");


      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNotNull(film2actorVertexType);

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("name"));
      assertEquals("name", actorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(2, actorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("surname"));
      assertEquals("surname", actorVertexType.getPropertyByName("surname").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("surname").getPropertyType());
      assertEquals(3, actorVertexType.getPropertyByName("surname").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("surname").isFromPrimaryKey());

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getPropertyType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("year"));
      assertEquals("year", filmVertexType.getPropertyByName("year").getName());
      assertEquals("DATE", filmVertexType.getPropertyByName("year").getPropertyType());
      assertEquals(3, filmVertexType.getPropertyByName("year").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("year").isFromPrimaryKey());

      assertEquals(2, film2actorVertexType.getProperties().size());

      assertNotNull(film2actorVertexType.getPropertyByName("filmId"));
      assertEquals("filmId", film2actorVertexType.getPropertyByName("filmId").getName());
      assertEquals("VARCHAR", film2actorVertexType.getPropertyByName("filmId").getPropertyType());
      assertEquals(1, film2actorVertexType.getPropertyByName("filmId").getOrdinalPosition());
      assertEquals(true, film2actorVertexType.getPropertyByName("filmId").isFromPrimaryKey());

      assertNotNull(film2actorVertexType.getPropertyByName("actorId"));
      assertEquals("actorId", film2actorVertexType.getPropertyByName("actorId").getName());
      assertEquals("VARCHAR", film2actorVertexType.getPropertyByName("actorId").getPropertyType());
      assertEquals(2, film2actorVertexType.getPropertyByName("actorId").getOrdinalPosition());
      assertEquals(true, film2actorVertexType.getPropertyByName("actorId").isFromPrimaryKey());

      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(filmEdgeType);
      assertNotNull(actorEdgeType);

      assertEquals("HasFilm", filmEdgeType.getName());
      assertEquals(0, filmEdgeType.getProperties().size());
      assertEquals("Film", filmEdgeType.getInVertexType().getName());
      assertEquals(1, filmEdgeType.getNumberRelationshipsRepresented());

      assertEquals("HasActor", actorEdgeType.getName());
      assertEquals(0, actorEdgeType.getProperties().size());
      assertEquals("Actor", actorEdgeType.getInVertexType().getName());
      assertEquals(1, actorEdgeType.getNumberRelationshipsRepresented());

    }catch(Exception e) {
      e.printStackTrace();
    }finally {
      try {

        // Dropping Source DB Schema and OrientGraph
        String dbDropping = "drop schema public cascade";
        st.execute(dbDropping);
        connection.close();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
  }

}
