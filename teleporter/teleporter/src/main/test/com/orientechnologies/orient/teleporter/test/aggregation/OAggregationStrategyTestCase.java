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

package com.orientechnologies.orient.teleporter.test.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.plugin.teleporter.context.OOutputStreamManager;
import com.orientechnologies.plugin.teleporter.context.OTeleporterContext;
import com.orientechnologies.plugin.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.plugin.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.plugin.teleporter.strategy.ONaiveAggregationImportStrategy;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OAggregationStrategyTestCase {

  private OTeleporterContext context;
  private ONaiveAggregationImportStrategy importStrategy;
  private String outOrientGraphUri;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.importStrategy = new ONaiveAggregationImportStrategy();
    this.outOrientGraphUri = "memory:testOrientDB";
  }


  @Test
  /*
   * Aggregation Strategy Test
   */
  public void test1() {
    
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

      String filmFilling = "INSERT INTO FILM (ID,TITLE) VALUES ("
          + "('F001','The Wolf Of Wall Street'),"
          + "('F002','Shutter Island'),"
          + "('F003','The Departed'),"
          + "('F004','Inception'))";
      st.execute(filmFilling);

      String actorFilling = "INSERT INTO ACTOR (ID,NAME,SURNAME) VALUES ("
          + "('A001','Leonardo','Di Caprio'),"
          + "('A002','Matthew', 'McConaughey'),"
          + "('A003','Ben','Kingsley'),"
          + "('A004','Mark','Ruffalo'),"
          + "('A005','Jack','Nicholson'),"
          + "('A006','Matt','Damon'),"
          + "('A007','Michael','Caine'))";
      st.execute(actorFilling);

      String film2actorFilling = "INSERT INTO FILM_ACTOR (FILM_ID,ACTOR_ID) VALUES ("
          + "('F001','A001'),"
          + "('F001','A002'),"
          + "('F002','A001'),"
          + "('F002','A003'),"
          + "('F002','A004'),"
          + "('F003','A001'),"
          + "('F003','A005'),"
          + "('F003','A006'),"
          + "('F004','A001'),"
          + "('F004','A007'))";
      st.execute(film2actorFilling);


      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, context);


      /*
       *  Testing context information
       */

      assertEquals(21, context.getStatistics().totalNumberOfRecords);
      assertEquals(21, context.getStatistics().importedRecords);
      assertEquals(11, context.getStatistics().orientVertices);
      assertEquals(10, context.getStatistics().orientEdges);

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
      assertEquals(11, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Film")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(4, count);

      count = 0;
      for(Vertex v: orientGraph.getVerticesOfClass("Actor")) {
        assertNotNull(v.getId());
        count++;
      }
      assertEquals(7, count);

      assertNull(orientGraph.getVertexType("FilmActor"));


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

      
      // vertex properties and connections check
      Iterator<Edge> edgesIt = null;
      String[] keys = {"id"};
      String[] values = {"F001"};
      
      Vertex v = null;
      Iterator<Vertex> iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F001", v.getProperty("id"));
        assertEquals("The Wolf Of Wall Street", v.getProperty("title"));
        edgesIt = v.getEdges(Direction.IN, "HasFilm").iterator();
        assertEquals("A001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("A002", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F002";
      iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F002", v.getProperty("id"));
        assertEquals("Shutter Island", v.getProperty("title"));
        edgesIt = v.getEdges(Direction.IN, "HasFilm").iterator();
        assertEquals("A001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("A003", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("A004", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "F003";
      iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F003", v.getProperty("id"));
        assertEquals("The Departed", v.getProperty("title"));
        edgesIt = v.getEdges(Direction.IN, "HasFilm").iterator();
        assertEquals("A001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("A005", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("A006", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }
      
      values[0] = "F004";
      iterator = orientGraph.getVertices("Film", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("F004", v.getProperty("id"));
        assertEquals("Inception", v.getProperty("title"));
        edgesIt = v.getEdges(Direction.IN, "HasFilm").iterator();
        assertEquals("A001", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals("A007", edgesIt.next().getVertex(Direction.OUT).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A001";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A001", v.getProperty("id"));
        assertEquals("Leonardo", v.getProperty("name"));
        assertEquals("Di Caprio", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "HasFilm").iterator();
        assertEquals("F001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals("F002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals("F003", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals("F004", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A002";
      iterator =  orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A002", v.getProperty("id"));
        assertEquals("Matthew", v.getProperty("name"));
        assertEquals("McConaughey", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "HasFilm").iterator();
        assertEquals("F001", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A003";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A003", v.getProperty("id"));
        assertEquals("Ben", v.getProperty("name"));
        assertEquals("Kingsley", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "HasFilm").iterator();
        assertEquals("F002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A004";
      iterator =  orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A004", v.getProperty("id"));
        assertEquals("Mark", v.getProperty("name"));
        assertEquals("Ruffalo", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "HasFilm").iterator();
        assertEquals("F002", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A005";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A005", v.getProperty("id"));
        assertEquals("Jack", v.getProperty("name"));
        assertEquals("Nicholson", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "HasFilm").iterator();
        assertEquals("F003", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A006";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A006", v.getProperty("id"));
        assertEquals("Matt", v.getProperty("name"));
        assertEquals("Damon", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "HasFilm").iterator();
        assertEquals("F003", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

      values[0] = "A007";
      iterator = orientGraph.getVertices("Actor", keys, values).iterator();
      assertTrue(iterator.hasNext());
      if(iterator.hasNext()) {
        v = iterator.next();
        assertEquals("A007", v.getProperty("id"));
        assertEquals("Michael", v.getProperty("name"));
        assertEquals("Caine", v.getProperty("surname"));
        edgesIt = v.getEdges(Direction.OUT, "HasFilm").iterator();
        assertEquals("F004", edgesIt.next().getVertex(Direction.IN).getProperty("id"));
        assertEquals(false, edgesIt.hasNext());
      }
      else {
        fail("Query fail!");
      }

    } catch(Exception e) {
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
      orientGraph.drop();
      orientGraph.shutdown();
    }  
  }

}
