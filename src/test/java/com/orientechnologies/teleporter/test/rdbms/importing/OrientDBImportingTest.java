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

package com.orientechnologies.teleporter.test.rdbms.importing;

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
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
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
 * @email gabriele.ponzi--at--gmail.com
 */

public class OrientDBImportingTest {

  private OTeleporterContext context;
  private ODBMSNaiveStrategy importStrategy;
  private ODBQueryEngine     dbQueryEngine;
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
    this.context = OTeleporterContext.newInstance(this.outParentDirectory);
    this.context.initOrientDBInstance(outOrientGraphUri);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.importStrategy = new ODBMSNaiveStrategy("embedded", this.outParentDirectory, this.dbName);
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
  public void test1() {

    Connection connection = null;
    Statement st = null;
    ODatabaseDocument orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      // Tables Building

      String directorTableBuilding = "create memory table DIRECTOR (ID varchar(256) not null, NAME  varchar(256),"
          + " SURNAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(directorTableBuilding);

      String categoryTableBuilding = "create memory table CATEGORY (ID varchar(256) not null, NAME  varchar(256), primary key (ID))";
      st.execute(categoryTableBuilding);

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"
          + " TITLE varchar(256) not null, DIRECTOR varchar(256) not null, CATEGORY varchar(256) not null," + " primary key (ID), "
          + " foreign key (DIRECTOR) references DIRECTOR(ID)," + " foreign key (CATEGORY) references CATEGORY(ID))";
      st.execute(filmTableBuilding);

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"
          + " SURNAME varchar(256) not null, primary key (ID))";
      st.execute(actorTableBuilding);

      String film2actorTableBuilding = "create memory table FILM_ACTOR (FILM_ID varchar(256) not null, ACTOR_ID  varchar(256),"
          + " primary key (FILM_ID,ACTOR_ID), foreign key (FILM_ID) references FILM(ID), foreign key (ACTOR_ID) references ACTOR(ID))";
      st.execute(film2actorTableBuilding);

      // Records Inserting

      String directorFilling =
          "insert into DIRECTOR (ID,NAME,SURNAME) values (" + "('D001','Quentin','Tarantino')," + "('D002','Martin','Scorsese'))";
      st.execute(directorFilling);

      String categoryFilling =
          "insert into CATEGORY (ID,NAME) values (" + "('C001','Thriller')," + "('C002','Action')," + "('C003','Sci-Fi'),"
              + "('C004','Fantasy')," + "('C005','Comedy')," + "('C006','Drama')," + "('C007','War'))";
      st.execute(categoryFilling);

      String filmFilling = "insert into FILM (ID,TITLE,DIRECTOR,CATEGORY) values (" + "('F001','Pulp Fiction','D001','C002'),"
          + "('F002','Shutter Island','D002','C001')," + "('F003','The Departed','D002','C001'))";
      st.execute(filmFilling);

      String actorFilling =
          "insert into ACTOR (ID,NAME,SURNAME) values (" + "('A001','John','Travolta')," + "('A002','Samuel','Lee Jackson'),"
              + "('A003','Bruce','Willis')," + "('A004','Leonardo','Di Caprio')," + "('A005','Ben','Kingsley'),"
              + "('A006','Mark','Ruffalo')," + "('A007','Jack','Nicholson')," + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling =
          "insert into FILM_ACTOR (FILM_ID,ACTOR_ID) values (" + "('F001','A001')," + "('F001','A002')," + "('F001','A003'),"
              + "('F002','A004')," + "('F002','A005')," + "('F002','A006')," + "('F003','A004')," + "('F003','A007'),"
              + "('F003','A008'))";
      st.execute(film2actorFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null);


      /*
       *  Testing context information
       */

      assertEquals(29, context.getStatistics().totalNumberOfRecords);
      assertEquals(29, context.getStatistics().analyzedRecords);
      assertEquals(29, context.getStatistics().orientAddedVertices);
      assertEquals(24, context.getStatistics().orientAddedEdges);


      /*
       *  Testing built OrientDB
       */

      orientGraph = this.context.getOrientDBInstance().open(this.dbName,"admin","admin");

      // vertices check

      assertEquals(29, orientGraph.countClass("V"));
      assertEquals(2, orientGraph.countClass("Director"));
      assertEquals(7, orientGraph.countClass("Category"));
      assertEquals(3, orientGraph.countClass("Film"));
      assertEquals(8, orientGraph.countClass("Actor"));
      assertEquals(9, orientGraph.countClass("FilmActor"));

      // edges check

      assertEquals(24, orientGraph.countClass("E"));
      assertEquals(3, orientGraph.countClass("HasDirector"));
      assertEquals(3, orientGraph.countClass("HasCategory"));
      assertEquals(9, orientGraph.countClass("HasFilm"));
      assertEquals(9, orientGraph.countClass("HasActor"));

      // vertex properties and connections check
      Iterator<OEdge>  edgesIt = null;
      String[] keys = { "id" };
      String[] values = { "D001" };

      OVertex v = null;
      OResultSet result = OGraphCommands.getVertices(orientGraph, "Director", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("D001", v.getProperty("id"));
        assertEquals("Quentin", v.getProperty("name"));
        assertEquals("Tarantino", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasDirector").iterator();
        assertEquals("F001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "D002";
      result = OGraphCommands.getVertices(orientGraph, "Director", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("D002", v.getProperty("id"));
        assertEquals("Martin", v.getProperty("name"));
        assertEquals("Scorsese", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasDirector").iterator();
        assertEquals("F002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals("F003", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "C001";
      result = OGraphCommands.getVertices(orientGraph, "Category", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("C001", v.getProperty("id"));
        assertEquals("Thriller", v.getProperty("name"));
        edgesIt = v.getEdges(ODirection.IN, "HasCategory").iterator();
        assertEquals("F002", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals("F003", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "C002";
      result = OGraphCommands.getVertices(orientGraph, "Category", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("C002", v.getProperty("id"));
        assertEquals("Action", v.getProperty("name"));
        edgesIt = v.getEdges(ODirection.IN, "HasCategory").iterator();
        assertEquals("F001", edgesIt.next().getVertex(ODirection.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "C003";
      result = OGraphCommands.getVertices(orientGraph, "Category", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("C003", v.getProperty("id"));
        assertEquals("Sci-Fi", v.getProperty("name"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "C004";
      result = OGraphCommands.getVertices(orientGraph, "Category", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("C004", v.getProperty("id"));
        assertEquals("Fantasy", v.getProperty("name"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "C005";
      result = OGraphCommands.getVertices(orientGraph, "Category", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("C005", v.getProperty("id"));
        assertEquals("Comedy", v.getProperty("name"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "C006";
      result = OGraphCommands.getVertices(orientGraph, "Category", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("C006", v.getProperty("id"));
        assertEquals("Drama", v.getProperty("name"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "C007";
      result = OGraphCommands.getVertices(orientGraph, "Category", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("C007", v.getProperty("id"));
        assertEquals("War", v.getProperty("name"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "F001";
      result = OGraphCommands.getVertices(orientGraph, "Film", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("Pulp Fiction", v.getProperty("title"));
        assertEquals("D001", v.getProperty("director"));
        assertEquals("C002", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "HasFilm").iterator();
        assertEquals("A001", edgesIt.next().getVertex(ODirection.OUT).getProperty("actorId"));
        assertEquals("A002", edgesIt.next().getVertex(ODirection.OUT).getProperty("actorId"));
        assertEquals("A003", edgesIt.next().getVertex(ODirection.OUT).getProperty("actorId"));
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
        assertEquals("D002", v.getProperty("director"));
        assertEquals("C001", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "HasFilm").iterator();
        assertEquals("A004", edgesIt.next().getVertex(ODirection.OUT).getProperty("actorId"));
        assertEquals("A005", edgesIt.next().getVertex(ODirection.OUT).getProperty("actorId"));
        assertEquals("A006", edgesIt.next().getVertex(ODirection.OUT).getProperty("actorId"));
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
        assertEquals("D002", v.getProperty("director"));
        assertEquals("C001", v.getProperty("category"));
        edgesIt = v.getEdges(ODirection.IN, "HasFilm").iterator();
        assertEquals("A004", edgesIt.next().getVertex(ODirection.OUT).getProperty("actorId"));
        assertEquals("A007", edgesIt.next().getVertex(ODirection.OUT).getProperty("actorId"));
        assertEquals("A008", edgesIt.next().getVertex(ODirection.OUT).getProperty("actorId"));
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
        assertEquals("John", v.getProperty("name"));
        assertEquals("Travolta", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasActor").iterator();
        assertEquals("F001", edgesIt.next().getVertex(ODirection.OUT).getProperty("filmId"));
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
        assertEquals("Samuel", v.getProperty("name"));
        assertEquals("Lee Jackson", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasActor").iterator();
        assertEquals("F001", edgesIt.next().getVertex(ODirection.OUT).getProperty("filmId"));
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
        assertEquals("Bruce", v.getProperty("name"));
        assertEquals("Willis", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasActor").iterator();
        assertEquals("F001", edgesIt.next().getVertex(ODirection.OUT).getProperty("filmId"));
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
        assertEquals("Leonardo", v.getProperty("name"));
        assertEquals("Di Caprio", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasActor").iterator();
        assertEquals("F002", edgesIt.next().getVertex(ODirection.OUT).getProperty("filmId"));
        assertEquals("F003", edgesIt.next().getVertex(ODirection.OUT).getProperty("filmId"));
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
        assertEquals("Ben", v.getProperty("name"));
        assertEquals("Kingsley", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasActor").iterator();
        assertEquals("F002", edgesIt.next().getVertex(ODirection.OUT).getProperty("filmId"));
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
        assertEquals("Mark", v.getProperty("name"));
        assertEquals("Ruffalo", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasActor").iterator();
        assertEquals("F002", edgesIt.next().getVertex(ODirection.OUT).getProperty("filmId"));
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
        assertEquals("Jack", v.getProperty("name"));
        assertEquals("Nicholson", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasActor").iterator();
        assertEquals("F003", edgesIt.next().getVertex(ODirection.OUT).getProperty("filmId"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      values[0] = "A008";
      result = OGraphCommands.getVertices(orientGraph, "Actor", keys, values);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("A008", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("name"));
        assertEquals("Damon", v.getProperty("surname"));
        edgesIt = v.getEdges(ODirection.IN, "HasActor").iterator();
        assertEquals("F003", edgesIt.next().getVertex(ODirection.OUT).getProperty("filmId"));
        assertEquals(false, edgesIt.hasNext());
      } else {
        fail("Query fail!");
      }

      String[] keys2 = { "filmId", "actorId" };
      String[] values2 = { "F001", "A001" };
      result = OGraphCommands.getVertices(orientGraph, "FilmActor", keys2, values2);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F001", v.getProperty("filmId"));
        assertEquals("A001", v.getProperty("actorId"));
      } else {
        fail("Query fail!");
      }

      values2[0] = "F001";
      values2[1] = "A002";
      result = OGraphCommands.getVertices(orientGraph, "FilmActor", keys2, values2);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F001", v.getProperty("filmId"));
        assertEquals("A002", v.getProperty("actorId"));
      } else {
        fail("Query fail!");
      }

      values2[0] = "F001";
      values2[1] = "A003";
      result = OGraphCommands.getVertices(orientGraph, "FilmActor", keys2, values2);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F001", v.getProperty("filmId"));
        assertEquals("A003", v.getProperty("actorId"));
      } else {
        fail("Query fail!");
      }

      values2[0] = "F002";
      values2[1] = "A004";
      result = OGraphCommands.getVertices(orientGraph, "FilmActor", keys2, values2);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F002", v.getProperty("filmId"));
        assertEquals("A004", v.getProperty("actorId"));
      } else {
        fail("Query fail!");
      }

      values2[0] = "F002";
      values2[1] = "A005";
      result = OGraphCommands.getVertices(orientGraph, "FilmActor", keys2, values2);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F002", v.getProperty("filmId"));
        assertEquals("A005", v.getProperty("actorId"));
      } else {
        fail("Query fail!");
      }

      values2[0] = "F002";
      values2[1] = "A006";
      result = OGraphCommands.getVertices(orientGraph, "FilmActor", keys2, values2);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F002", v.getProperty("filmId"));
        assertEquals("A006", v.getProperty("actorId"));
      } else {
        fail("Query fail!");
      }

      values2[0] = "F003";
      values2[1] = "A004";
      result = OGraphCommands.getVertices(orientGraph, "FilmActor", keys2, values2);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F003", v.getProperty("filmId"));
        assertEquals("A004", v.getProperty("actorId"));
      } else {
        fail("Query fail!");
      }

      values2[0] = "F003";
      values2[1] = "A007";
      result = OGraphCommands.getVertices(orientGraph, "FilmActor", keys2, values2);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F003", v.getProperty("filmId"));
        assertEquals("A007", v.getProperty("actorId"));
      } else {
        fail("Query fail!");
      }

      values2[0] = "F003";
      values2[1] = "A008";
      result = OGraphCommands.getVertices(orientGraph, "FilmActor", keys2, values2);
      assertTrue(result.hasNext());
      if (result.hasNext()) {
        v = result.next().getVertex().get();
        assertEquals("F003", v.getProperty("filmId"));
        assertEquals("A008", v.getProperty("actorId"));
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
