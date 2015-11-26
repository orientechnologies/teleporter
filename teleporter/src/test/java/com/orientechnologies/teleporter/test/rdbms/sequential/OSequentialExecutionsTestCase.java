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

package com.orientechnologies.teleporter.test.rdbms.sequential;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveAggregationStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OSequentialExecutionsTestCase {

  private OTeleporterContext context;
  private ODBMSNaiveStrategy naiveImportStrategy;
  private ODBMSNaiveAggregationStrategy naiveAggregationImportStrategy;
  private String outOrientGraphUri;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.context.setQueryQuoteType("\"");
    this.naiveImportStrategy = new ODBMSNaiveStrategy();
    this.naiveAggregationImportStrategy = new ODBMSNaiveAggregationStrategy();
    this.outOrientGraphUri = "memory:testOrientDB";
  }


  @Test

  /*
   *  Adding a column to a table.
   */

  public void schemaModificationTest1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(actorTableBuilding);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(3, actorVertexType.properties().size());

      OProperty currentProperty = null;
      Iterator<OProperty> itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());


      /*
       * Modify of the db schema, adding a column
       */

      String addColumn = "alter table ACTOR "
          + "add BIRTHDAY date";
      st.execute(addColumn);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(4, actorVertexType.properties().size());

      currentProperty = null;
      itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("birthday", currentProperty.getName());
      assertEquals(OType.DATE, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());


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
      orientGraph.drop();
      orientGraph.shutdown();
    }  
  }



  @Test

  /*
   *  Removing a column from a table.
   */

  public void schemaModificationTest2() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, birthday date, primary key (ID))";
      st = connection.createStatement();
      st.execute(actorTableBuilding);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(4, actorVertexType.properties().size());

      OProperty currentProperty = null;
      Iterator<OProperty> itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("birthday", currentProperty.getName());
      assertEquals(OType.DATE, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());


      /*
       * Modify of the db schema, removing a column
       */

      String removeColumn = "alter table ACTOR "
          + "drop column BIRTHDAY";
      st.execute(removeColumn);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(3, actorVertexType.properties().size());

      currentProperty = null;
      itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());


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
      orientGraph.drop();
      orientGraph.shutdown();
    }  
  }

  @Test

  /*
   *  Modify of a column.
   */

  public void schemaModificationTest3() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, birthday varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(actorTableBuilding);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(4, actorVertexType.properties().size());

      OProperty currentProperty = null;
      Iterator<OProperty> itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("birthday", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());


      /*
       * Modify of the db schema, changing a column (name and type)
       */

      String modifyColumn = "alter table ACTOR "
          + "alter column BIRTHDAY date";
      st.execute(modifyColumn);

      modifyColumn = "alter table ACTOR "
          + "alter column BIRTHDAY rename to ANNIVERSARY";
      st.execute(modifyColumn);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(4, actorVertexType.properties().size());

      currentProperty = null;
      itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("anniversary", currentProperty.getName());
      assertEquals(OType.DATE, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());


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
      orientGraph.drop();
      orientGraph.shutdown();
    }  
  }


  @Test

  /*
   *  Modify of a relationship (foreign key).
   */

  public void schemaModificationTest4() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String directorTableBuilding = "create memory table DIRECTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, BESTFILM varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(directorTableBuilding);

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, DIRECTOR varchar(256) not null," +
          " primary key (ID), constraint director foreign key (DIRECTOR) references DIRECTOR(ID))";
      st.execute(filmTableBuilding);


      // Records Inserting

      String directorFilling = "insert into DIRECTOR (ID,NAME,SURNAME,BESTFILM) values ("
          + "('D001','Quentin','Tarantino','F001'),"
          + "('D002','Martin','Scorsese','F002'))";
      st.execute(directorFilling);

      String filmFilling = "insert into FILM (ID,TITLE,DIRECTOR) values ("
          + "('F001','Pulp Fiction','D001'),"
          + "('F002','Shutter Island','D002'))";
      st.execute(filmFilling);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Director")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
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
      assertEquals(2, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasDirector")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);

      
      /*
       * Modify of the db schema, adding a table
       */

      String dropOldForeignKey = "alter table FILM drop constraint director";
      st.execute(dropOldForeignKey);

      String addNewForeignKey = "alter table DIRECTOR add foreign key (BESTFILM) references FILM(ID)";
      st.execute(addNewForeignKey);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Director")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
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
      assertEquals(4, count);
      
      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasDirector")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasBestfilm")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);




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
      orientGraph.drop();
      orientGraph.shutdown();
    }  

  }





  @Test

  /*
   *  Adding a table.
   */

  public void schemaModificationTest5() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, birthday varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(actorTableBuilding);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(4, actorVertexType.properties().size());

      OProperty currentProperty = null;
      Iterator<OProperty> itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("birthday", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());


      /*
       * Modify of the db schema, adding a table
       */

      String addTable = "create memory table FILM (ID varchar(256) not null, "+
          "TITLE varchar(256) not null, DIRECTOR varchar(256) not null, primary key (ID))";
      st.execute(addTable);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(4, actorVertexType.properties().size());

      currentProperty = null;
      itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("birthday", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());

      OrientVertexType filmVertexType = orientGraph.getVertexType("Film");

      assertNotNull(filmVertexType);
      assertEquals(3, filmVertexType.properties().size());

      currentProperty = null;
      itProperties = filmVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("title", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("director", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());


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
      orientGraph.drop();
      orientGraph.shutdown();
    } 
  }


  @Test

  /*
   *  Adding a relationship (foreign key).
   */

  public void schemaModificationTest6() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String directorTableBuilding = "create memory table DIRECTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(directorTableBuilding);

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, DIRECTOR varchar(256) not null, primary key (ID))";
      st.execute(filmTableBuilding);


      // Records Inserting

      String directorFilling = "insert into DIRECTOR (ID,NAME,SURNAME) values ("
          + "('D001','Quentin','Tarantino'),"
          + "('D002','Martin','Scorsese'))";
      st.execute(directorFilling);

      String filmFilling = "insert into FILM (ID,TITLE,DIRECTOR) values ("
          + "('F001','Pulp Fiction','D001'),"
          + "('F002','Shutter Island','D002'))";
      st.execute(filmFilling);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Director")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
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
      assertEquals(0, count);


      /*
       *  Adding a relationship (foreign key).
       */

      String addNewForeignKey = "alter table FILM add foreign key (DIRECTOR) references DIRECTOR(ID)";
      st.execute(addNewForeignKey);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Director")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
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
      assertEquals(2, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasDirector")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);


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
      orientGraph.drop();
      orientGraph.shutdown();
    }  

  }

  @Test

  /*
   *  Removing a table.
   */

  public void schemaModificationTest7() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, birthday varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(actorTableBuilding);

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, DIRECTOR varchar(256) not null, primary key (ID))";
      st.execute(filmTableBuilding);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      OrientVertexType actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(4, actorVertexType.properties().size());

      OProperty currentProperty = null;
      Iterator<OProperty> itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("birthday", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());

      OrientVertexType filmVertexType = orientGraph.getVertexType("Film");

      assertNotNull(filmVertexType);
      assertEquals(3, filmVertexType.properties().size());

      currentProperty = null;
      itProperties = filmVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("title", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("director", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());


      /*
       * Modify of the db schema, removing a table
       */

      String removeTable = "drop table FILM";
      st.execute(removeTable);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      actorVertexType = orientGraph.getVertexType("Actor");

      assertNotNull(actorVertexType);
      assertEquals(4, actorVertexType.properties().size());

      currentProperty = null;
      itProperties = actorVertexType.properties().iterator();
      currentProperty = itProperties.next();
      assertEquals("id", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("birthday", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("name", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      currentProperty = itProperties.next();
      assertEquals("surname", currentProperty.getName());
      assertEquals(OType.STRING, currentProperty.getType());
      assertFalse(itProperties.hasNext());

      filmVertexType = orientGraph.getVertexType(filmTableBuilding);

      assertNull(filmVertexType);



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
      orientGraph.drop();
      orientGraph.shutdown();
    } 
  }


  @Test

  /*
   *  Removing a relationship (foreign key).
   */

  public void schemaModificationTest8() {


    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String directorTableBuilding = "create memory table DIRECTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(directorTableBuilding);

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null," +
          " TITLE varchar(256) not null, DIRECTOR varchar(256) not null, primary key (ID)," +
          " constraint director foreign key (DIRECTOR) references DIRECTOR(ID))";
      st.execute(filmTableBuilding);


      // Records Inserting

      String directorFilling = "insert into DIRECTOR (ID,NAME,SURNAME) values ("
          + "('D001','Quentin','Tarantino'),"
          + "('D002','Martin','Scorsese'))";
      st.execute(directorFilling);

      String filmFilling = "insert into FILM (ID,TITLE,DIRECTOR) values ("
          + "('F001','Pulp Fiction','D001'),"
          + "('F002','Shutter Island','D002'))";
      st.execute(filmFilling);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Director")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
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
      assertEquals(2, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasDirector")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);


      /*
       *  Removing a relation 
       */

      String dropForeignKey = "alter table FILM drop constraint director";
      st.execute(dropForeignKey);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Director")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
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
      assertEquals(2, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasDirector")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(2, count);


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
      orientGraph.drop();
      orientGraph.shutdown();
    } 

  }



  @Test

  /*
   * Adding records to a table.
   */

  public void contentModificationTest1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, birthday varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(actorTableBuilding);


      // Records Inserting

      String actorFilling = "insert into ACTOR (ID,NAME,SURNAME) values ("
          + "('A001','John','Travolta'),"
          + "('A002','Samuel','Lee Jackson'),"
          + "('A003','Bruce','Willis'),"
          + "('A004','Leonardo','Di Caprio'),"
          + "('A005','Ben','Kingsley'),"
          + "('A006','Mark','Ruffalo'),"
          + "('A007','Jack','Nicholson'),"
          + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);

      // edges check

      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(0, count);

      
      /*
       * Adding records to the film table
       */

      actorFilling = "insert into ACTOR (ID,NAME,SURNAME) values ("
          + "('A009','Christian','Bale'),"
          + "('A010','Hugh','Jackman'))";
      st.executeQuery(actorFilling);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(10, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(10, count);

      // edges check

      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(0, count);

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
      orientGraph.drop();
      orientGraph.shutdown();
    } 
  }

  @Test

  /*
   * Adding records to a join table (aggregation strategy).
   */

  public void contentModificationTest2() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, primary key (ID))";
      st.execute(actorTableBuilding);

      String film2actorTableBuilding = "create memory table FILM_ACTOR (FILM_ID varchar(256) not null, ACTOR_ID  varchar(256),"+
          " primary key (FILM_ID,ACTOR_ID), foreign key (FILM_ID) references FILM(ID), foreign key (ACTOR_ID) references ACTOR(ID))";
      st.execute(film2actorTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE) values ("
          + "('F001','The Wolf Of Wall Street'),"
          + "('F002','Shutter Island'),"
          + "('F003','The Departed'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,NAME,SURNAME) values ("
          + "('A001','Leonardo','Di Caprio'),"
          + "('A002','Matthew', 'McConaughey'),"
          + "('A003','Ben','Kingsley'),"
          + "('A004','Mark','Ruffalo'),"
          + "('A005','Jack','Nicholson'),"
          + "('A006','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "insert into FILM_ACTOR (FILM_ID,ACTOR_ID) values ("
          + "('F001','A001'),"
          + "('F001','A002'),"
          + "('F002','A001'),"
          + "('F002','A003'),"
          + "('F002','A004'),"
          + "('F003','A001'),"
          + "('F003','A005'),"
          + "('F003','A006'))";
      st.execute(film2actorFilling);

      this.naiveAggregationImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(9, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(6, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      // edges check

      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(8, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasFilm")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(8, count);


      /*
       * Adding records to the film, actor and join tables
       */

      actorFilling = "insert into ACTOR (ID,NAME,SURNAME) values ("
          + "('A007','Michael','Caine'))";
      st.executeQuery(actorFilling);

      filmFilling = "insert into FILM (ID,TITLE) values ("
          + "('F004','Inception'))";
      st.executeQuery(filmFilling);

      film2actorFilling = "insert into FILM_ACTOR (FILM_ID,ACTOR_ID) values ("
          + "('F004','A001'),"
          + "('F004','A007'))";
      st.executeQuery(film2actorFilling);

      this.naiveAggregationImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(11, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(7, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      // edges check

      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(10, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasFilm")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(10, count);


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
      orientGraph.drop();
      orientGraph.shutdown();
    } 
  }

  @Test

  /*
   * Adding a table with records.
   */

  public void contentModificationTest3() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String directorTableBuilding = "create memory table DIRECTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(directorTableBuilding);

      // Records Inserting

      String directorFilling = "insert into DIRECTOR (ID,NAME,SURNAME) values ("
          + "('D001','Quentin','Tarantino'),"
          + "('D002','Martin','Scorsese'))";
      st.execute(directorFilling);    

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Director")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);
      

      /*
       * Adding a table with records e foreign key
       */

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, DIRECTOR varchar(256) not null, primary key (ID), " +
          " foreign key (DIRECTOR) references DIRECTOR(ID))";
      st.executeQuery(filmTableBuilding);

      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,DIRECTOR) values ("
          + "('F001','Pulp Fiction','D001'),"
          + "('F002','Shutter Island','D002'),"
          + "('F003','The Departed','D002'))";
      st.execute(filmFilling);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(5, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Director")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      // edges check

      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasDirector")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(3, count);


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
      orientGraph.drop();
      orientGraph.shutdown();
    } 

  }

  @Test

  /*
   * Adding a join table with records (aggregation strategy).
   */

  public void contentModificationTest4() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, primary key (ID))";
      st.execute(actorTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE) values ("
          + "('F001','The Wolf Of Wall Street'),"
          + "('F002','Shutter Island'),"
          + "('F003','The Departed'))";
      st.execute(filmFilling);

      String actorFilling = "insert into ACTOR (ID,NAME,SURNAME) values ("
          + "('A001','Leonardo','Di Caprio'),"
          + "('A002','Matthew', 'McConaughey'),"
          + "('A003','Ben','Kingsley'),"
          + "('A004','Mark','Ruffalo'),"
          + "('A005','Jack','Nicholson'),"
          + "('A006','Matt','Damon'))";
      st.execute(actorFilling);

      this.naiveAggregationImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(9, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(6, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      // edges check

      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(0, count);


      /*
       * Adding a join table with records (aggregation strategy).
       */

      String film2actorTableBuilding = "create memory table FILM_ACTOR (FILM_ID varchar(256) not null, ACTOR_ID  varchar(256),"+
          " primary key (FILM_ID,ACTOR_ID), foreign key (FILM_ID) references FILM(ID), foreign key (ACTOR_ID) references ACTOR(ID))";
      st.execute(film2actorTableBuilding);

      String film2actorFilling = "insert into FILM_ACTOR (FILM_ID,ACTOR_ID) values ("
          + "('F001','A001'),"
          + "('F001','A002'),"
          + "('F002','A001'),"
          + "('F002','A003'),"
          + "('F002','A004'),"
          + "('F003','A001'),"
          + "('F003','A005'),"
          + "('F003','A006'))";
      st.execute(film2actorFilling);

      this.naiveAggregationImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(9, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(6, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      // edges check

      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(8, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasFilm")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(8, count);


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
      orientGraph.drop();
      orientGraph.shutdown();
    } 
  }


  @Test

  /*
   * Update of n records of a table.
   */

  public void contentModificationTest5() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, birthday varchar(256), primary key (ID))";
      st = connection.createStatement();
      st.execute(actorTableBuilding);


      // Records Inserting

      String actorFilling = "insert into ACTOR (ID,NAME,SURNAME) values ("
          + "('A001','John','Unaltravolta'),"
          + "('A002','Samuel','Lì Clacson'),"
          + "('A003','Bruce','Willis'),"
          + "('A004','Leonardo','Di Caprio'),"
          + "('A005','Ben','Kingsley'),"
          + "('A006','Mark','Ruffalo'),"
          + "('A007','Jack','Nicholson'),"
          + "('A008','Matto','Demone'))";
      st.execute(actorFilling);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      int count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);

      Iterator<Vertex> it = orientGraph.getVerticesOfClass("Actor").iterator();
      Vertex currentVertex = it.next();
      assertEquals("John", currentVertex.getProperty("name"));
      assertEquals("Unaltravolta", currentVertex.getProperty("surname"));
      currentVertex = it.next();
      assertEquals("Samuel", currentVertex.getProperty("name"));
      assertEquals("Lì Clacson", currentVertex.getProperty("surname"));
      currentVertex = it.next();
      currentVertex = it.next();
      currentVertex = it.next();
      currentVertex = it.next();
      currentVertex = it.next();
      currentVertex = it.next();
      assertEquals("Matto", currentVertex.getProperty("name"));
      assertEquals("Demone", currentVertex.getProperty("surname"));
      assertFalse(it.hasNext());


      // edges check

      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(0, count);


      /*
       * Update of n records of a table.
       */

      String update = "update ACTOR set surname='Travolta' where id='A001'";
      st.executeQuery(update);
      update = "update ACTOR set surname='Lee Jackson' where id='A002'";
      st.executeQuery(update);
      update = "update ACTOR set name='Matt', surname='Damon' where id='A008'";
      st.executeQuery(update);

      this.naiveImportStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper",  null, "java", null, null, context);


      /*
       *  Testing built OrientDB
       */

      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check

      count = 0;
      for(Vertex v: orientGraph.getVertices()) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);

      it = orientGraph.getVerticesOfClass("Actor").iterator();
      currentVertex = it.next();
      assertEquals("John", currentVertex.getProperty("name"));
      assertEquals("Travolta", currentVertex.getProperty("surname"));
      currentVertex = it.next();
      assertEquals("Samuel", currentVertex.getProperty("name"));
      assertEquals("Lee Jackson", currentVertex.getProperty("surname"));
      currentVertex = it.next();
      currentVertex = it.next();
      currentVertex = it.next();
      currentVertex = it.next();
      currentVertex = it.next();
      currentVertex = it.next();
      assertEquals("Matt", currentVertex.getProperty("name"));
      assertEquals("Damon", currentVertex.getProperty("surname"));
      assertFalse(it.hasNext());



      // edges check

      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(0, count);

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
      orientGraph.drop();
      orientGraph.shutdown();
    } 
  }

}
