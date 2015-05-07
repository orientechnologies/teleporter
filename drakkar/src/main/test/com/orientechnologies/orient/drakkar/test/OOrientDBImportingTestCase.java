package com.orientechnologies.orient.drakkar.test;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.OOutputStreamManager;
import com.orientechnologies.orient.drakkar.importengine.ODB2GraphImportEngine;
import com.orientechnologies.orient.drakkar.mapper.OER2GraphMapper;
import com.orientechnologies.orient.drakkar.mapper.OSource2GraphMapper;
import com.orientechnologies.orient.drakkar.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.orient.drakkar.persistence.handler.OGenericDataTypeHandler;
import com.orientechnologies.orient.drakkar.writer.OGraphModelWriter;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

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

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class OOrientDBImportingTestCase {

  private OSource2GraphMapper mapper;
  private ODrakkarContext context;
  private OGraphModelWriter modelWriter;
  private ODB2GraphImportEngine importEngine;
  private String outOrientGraphUri;

  @Before
  public void init() {
    this.context = new ODrakkarContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.modelWriter = new OGraphModelWriter();
    this.importEngine = new ODB2GraphImportEngine();
    this.outOrientGraphUri = "memory:testOrientDB";
  }


  @Test
  public void test1() {

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

      String categoryTableBuilding = "create memory table CATEGORY (ID varchar(256) not null, NAME  varchar(256), primary key (ID))";
      st.execute(categoryTableBuilding);

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, DIRECTOR varchar(256) not null, CATEGORY varchar(256) not null, primary key (ID), " +
          " foreign key (DIRECTOR) references DIRECTOR(ID)," + 
          " foreign key (CATEGORY) references CATEGORY(ID))";
      st.execute(filmTableBuilding);

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME  varchar(256),"+
          " SURNAME varchar(256) not null, primary key (ID))";
      st.execute(actorTableBuilding);

      String film2actorTableBuilding = "create memory table FILM_ACTOR (FILM_ID varchar(256) not null, ACTOR_ID  varchar(256),"+
          " primary key (FILM_ID,ACTOR_ID), foreign key (FILM_ID) references FILM(ID), foreign key (ACTOR_ID) references ACTOR(ID))";
      st.execute(film2actorTableBuilding);


      // Records Inserting
      String directorFilling = "INSERT INTO DIRECTOR (ID,NAME,SURNAME) VALUES ("
          + "('D001','Quentin','Tarantino'),"
          + "('D002','Martin','Scorsese'))";
      st.execute(directorFilling);

      String categoryFilling = "INSERT INTO CATEGORY (ID,NAME) VALUES ("
          + "('C001','Thriller'),"
          + "('C002','Action'),"
          + "('C003','Sci-Fi'),"
          + "('C004','Fantasy'),"
          + "('C005','Comedy'),"
          + "('C006','Drama'),"
          + "('C007','War'))";
      st.execute(categoryFilling);

      String filmFilling = "INSERT INTO FILM (ID,TITLE,DIRECTOR,CATEGORY) VALUES ("
          + "('F001','Pulp Fiction','D001','C002'),"
          + "('F002','Shutter Island','D002','C001'),"
          + "('F003','The Departed','D002','C001'))";
      st.execute(filmFilling);

      String actorFilling = "INSERT INTO ACTOR (ID,NAME,SURNAME) VALUES ("
          + "('A001','John','Travolta'),"
          + "('A002','Samuel','Lee Jackson'),"
          + "('A003','Bruce','Willis'),"
          + "('A004','Leonardo','Di Caprio'),"
          + "('A005','Ben','Kingsley'),"
          + "('A006','Mark','Ruffalo'),"
          + "('A007','Jack','Nicholson'),"
          + "('A008','Matt','Damon'))";
      st.execute(actorFilling);

      String film2actorFilling = "INSERT INTO FILM_ACTOR (FILM_ID,ACTOR_ID) VALUES ("
          + "('F001','A001'),"
          + "('F001','A002'),"
          + "('F001','A003'),"
          + "('F002','A004'),"
          + "('F002','A005'),"
          + "('F002','A006'),"
          + "('F003','A004'),"
          + "('F003','A007'),"
          + "('F003','A008'))";
      st.execute(film2actorFilling);


      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "");
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(((OER2GraphMapper)mapper).getGraphModel(), new OGenericDataTypeHandler(), this.outOrientGraphUri, context);
      this.importEngine.executeImport("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, this.mapper, new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(29, context.getStatistics().totalNumberOfRecords);
      assertEquals(29, context.getStatistics().importedRecords);

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
      assertEquals(29, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Director")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(2, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Category")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(7, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(8, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("FilmActor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(9, count);


      // edges check
      count = 0;
      for(Edge e: orientGraph.getEdges()) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(24, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasDirector")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasCategory")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(3, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasFilm")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);

      count = 0;
      for(Edge e: orientGraph.getEdgesOfClass("HasActor")) {
        assertNotNull(e.getId());
        count++;
      }
      assertEquals(9, count);


      // vertex properties and connections check
      Iterator<Edge> edgesIt;

      Vertex v = orientGraph.getVertexByKey("Director.pkey", "D001");
      assertEquals("D001", v.getProperty("id"));
      assertEquals("Quentin", v.getProperty("name"));
      assertEquals("Tarantino", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasDirector").iterator();
      assertEquals("F001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
      assertEquals(false, edgesIt.hasNext());


      v = orientGraph.getVertexByKey("Director.pkey", "D002");
      assertEquals("D002", v.getProperty("id"));
      assertEquals("Martin", v.getProperty("name"));
      assertEquals("Scorsese", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasDirector").iterator();
      assertEquals("F002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
      assertEquals("F003", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
      assertEquals(false, edgesIt.hasNext());


      v = orientGraph.getVertexByKey("Category.pkey", "C001");
      assertEquals("C001", v.getProperty("id"));
      assertEquals("Thriller", v.getProperty("name"));
      edgesIt = v.getEdges(Direction.IN, "HasCategory").iterator();
      assertEquals("F002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
      assertEquals("F003", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Category.pkey", "C002");
      assertEquals("C002", v.getProperty("id"));
      assertEquals("Action", v.getProperty("name"));
      edgesIt = v.getEdges(Direction.IN, "HasCategory").iterator();
      assertEquals("F001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
      assertEquals(false, edgesIt.hasNext());


      v = orientGraph.getVertexByKey("Category.pkey", "C003");
      assertEquals("C003", v.getProperty("id"));
      assertEquals("Sci-Fi", v.getProperty("name"));
      assertEquals(false, edgesIt.hasNext());


      v = orientGraph.getVertexByKey("Category.pkey", "C004");
      assertEquals("C004", v.getProperty("id"));
      assertEquals("Fantasy", v.getProperty("name"));
      assertEquals(false, edgesIt.hasNext());


      v = orientGraph.getVertexByKey("Category.pkey", "C005");
      assertEquals("C005", v.getProperty("id"));
      assertEquals("Comedy", v.getProperty("name"));
      assertEquals(false, edgesIt.hasNext());


      v = orientGraph.getVertexByKey("Category.pkey", "C006");
      assertEquals("C006", v.getProperty("id"));
      assertEquals("Drama", v.getProperty("name"));
      assertEquals(false, edgesIt.hasNext());


      v = orientGraph.getVertexByKey("Category.pkey", "C007");
      assertEquals("C007", v.getProperty("id"));
      assertEquals("War", v.getProperty("name"));
      assertEquals(false, edgesIt.hasNext());


      v = orientGraph.getVertexByKey("Film.pkey", "F001");
      assertEquals("F001", v.getProperty("id"));
      assertEquals("Pulp Fiction", v.getProperty("title"));
      assertEquals("D001", v.getProperty("director"));
      assertEquals("C002", v.getProperty("category"));
      edgesIt = v.getEdges(Direction.IN, "HasFilm").iterator();
      assertEquals("A001", edgesIt.next().getVertex(Direction.OUT).getProperty("actorId"));
      assertEquals("A002", edgesIt.next().getVertex(Direction.OUT).getProperty("actorId"));
      assertEquals("A003", edgesIt.next().getVertex(Direction.OUT).getProperty("actorId"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Film.pkey", "F002");
      assertEquals("F002", v.getProperty("id"));
      assertEquals("Shutter Island", v.getProperty("title"));
      assertEquals("D002", v.getProperty("director"));
      assertEquals("C001", v.getProperty("category"));
      edgesIt = v.getEdges(Direction.IN, "HasFilm").iterator();
      assertEquals("A004", edgesIt.next().getVertex(Direction.OUT).getProperty("actorId"));
      assertEquals("A005", edgesIt.next().getVertex(Direction.OUT).getProperty("actorId"));
      assertEquals("A006", edgesIt.next().getVertex(Direction.OUT).getProperty("actorId"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Film.pkey", "F003");
      assertEquals("F003", v.getProperty("id"));
      assertEquals("The Departed", v.getProperty("title"));
      assertEquals("D002", v.getProperty("director"));
      assertEquals("C001", v.getProperty("category"));
      edgesIt = v.getEdges(Direction.IN, "HasFilm").iterator();
      assertEquals("A004", edgesIt.next().getVertex(Direction.OUT).getProperty("actorId"));
      assertEquals("A007", edgesIt.next().getVertex(Direction.OUT).getProperty("actorId"));
      assertEquals("A008", edgesIt.next().getVertex(Direction.OUT).getProperty("actorId"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Actor.pkey", "A001");
      assertEquals("A001", v.getProperty("id"));
      assertEquals("John", v.getProperty("name"));
      assertEquals("Travolta", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasActor").iterator();
      assertEquals("F001", edgesIt.next().getVertex(Direction.OUT).getProperty("filmId"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Actor.pkey", "A002");
      assertEquals("A002", v.getProperty("id"));
      assertEquals("Samuel", v.getProperty("name"));
      assertEquals("Lee Jackson", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasActor").iterator();
      assertEquals("F001", edgesIt.next().getVertex(Direction.OUT).getProperty("filmId"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Actor.pkey", "A003");
      assertEquals("A003", v.getProperty("id"));
      assertEquals("Bruce", v.getProperty("name"));
      assertEquals("Willis", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasActor").iterator();
      assertEquals("F001", edgesIt.next().getVertex(Direction.OUT).getProperty("filmId"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Actor.pkey", "A004");
      assertEquals("A004", v.getProperty("id"));
      assertEquals("Leonardo", v.getProperty("name"));
      assertEquals("Di Caprio", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasActor").iterator();
      assertEquals("F002", edgesIt.next().getVertex(Direction.OUT).getProperty("filmId"));
      assertEquals("F003", edgesIt.next().getVertex(Direction.OUT).getProperty("filmId"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Actor.pkey", "A005");
      assertEquals("A005", v.getProperty("id"));
      assertEquals("Ben", v.getProperty("name"));
      assertEquals("Kingsley", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasActor").iterator();
      assertEquals("F002", edgesIt.next().getVertex(Direction.OUT).getProperty("filmId"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Actor.pkey", "A006");
      assertEquals("A006", v.getProperty("id"));
      assertEquals("Mark", v.getProperty("name"));
      assertEquals("Ruffalo", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasActor").iterator();
      assertEquals("F002", edgesIt.next().getVertex(Direction.OUT).getProperty("filmId"));
      assertEquals(false, edgesIt.hasNext());


      v = orientGraph.getVertexByKey("Actor.pkey", "A007");
      assertEquals("A007", v.getProperty("id"));
      assertEquals("Jack", v.getProperty("name"));
      assertEquals("Nicholson", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasActor").iterator();
      assertEquals("F003", edgesIt.next().getVertex(Direction.OUT).getProperty("filmId"));
      assertEquals(false, edgesIt.hasNext());

      v = orientGraph.getVertexByKey("Actor.pkey", "A008");
      assertEquals("A008", v.getProperty("id"));
      assertEquals("Matt", v.getProperty("name"));
      assertEquals("Damon", v.getProperty("surname"));
      edgesIt = v.getEdges(Direction.IN, "HasActor").iterator();
      assertEquals("F003", edgesIt.next().getVertex(Direction.OUT).getProperty("filmId"));
      assertEquals(false, edgesIt.hasNext());

      String[] keys = {"filmId","actorId"};
      String[] values = {"F001","A001"};
      Iterator<Vertex> iterator = orientGraph.getVertices("FilmActor", keys, values).iterator();
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F001", v.getProperty("filmId"));
        assertEquals("A001", v.getProperty("actorId"));
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F001";
      values[1] = "A002";
      iterator = orientGraph.getVertices("FilmActor", keys, values).iterator();
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F001", v.getProperty("filmId"));
        assertEquals("A002", v.getProperty("actorId"));
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F001";
      values[1] = "A003";
      iterator = orientGraph.getVertices("FilmActor", keys, values).iterator();
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F001", v.getProperty("filmId"));
        assertEquals("A003", v.getProperty("actorId"));
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F002";
      values[1] = "A004";
      iterator = orientGraph.getVertices("FilmActor", keys, values).iterator();
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F002", v.getProperty("filmId"));
        assertEquals("A004", v.getProperty("actorId"));
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F002";
      values[1] = "A005";
      iterator = orientGraph.getVertices("FilmActor", keys, values).iterator();
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F002", v.getProperty("filmId"));
        assertEquals("A005", v.getProperty("actorId"));
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F002";
      values[1] = "A006";
      iterator = orientGraph.getVertices("FilmActor", keys, values).iterator();
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F002", v.getProperty("filmId"));
        assertEquals("A006", v.getProperty("actorId"));
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F003";
      values[1] = "A004";
      iterator = orientGraph.getVertices("FilmActor", keys, values).iterator();
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F003", v.getProperty("filmId"));
        assertEquals("A004", v.getProperty("actorId"));
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F003";
      values[1] = "A007";
      iterator = orientGraph.getVertices("FilmActor", keys, values).iterator();
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F003", v.getProperty("filmId"));
        assertEquals("A007", v.getProperty("actorId"));
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F003";
      values[1] = "A008";
      iterator = orientGraph.getVertices("FilmActor", keys, values).iterator();
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F003", v.getProperty("filmId"));
        assertEquals("A008", v.getProperty("actorId"));
      }
      else {
        fail("Query fail!");
      }


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
