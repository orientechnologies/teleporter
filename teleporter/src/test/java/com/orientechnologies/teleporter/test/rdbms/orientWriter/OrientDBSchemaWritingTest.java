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

package com.orientechnologies.teleporter.test.rdbms.orientWriter;

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class OrientDBSchemaWritingTest {

  private OER2GraphMapper    mapper;
  private OTeleporterContext context;
  private OGraphModelWriter  modelWriter;
  private ODBQueryEngine dbQueryEngine;
  private String driver = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private String outOrientGraphUri = "memory:testOrientDB";
  private OSourceDatabaseInfo sourceDBInfo;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.dbQueryEngine = new ODBQueryEngine(this.driver, this.context);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.modelWriter = new OGraphModelWriter();
    this.outOrientGraphUri = "memory:testOrientDB";
    this.sourceDBInfo = new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
  }


  @Test

  /*
   *  Two tables Foreign and Parent with a simple primary key imported from the parent table.
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table BOOK_AUTHOR (ID varchar(256) not null,"+
          " NAME varchar(256) not null, AGE integer not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256),"+
          " AUTHOR_ID varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references BOOK_AUTHOR(ID))";
      st.execute(foreignTableBuilding);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null, null);
      mapper.buildSourceDatabaseSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfVertexTypes);
      assertEquals(2, context.getStatistics().wroteVertexType);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().wroteEdgeType);
      assertEquals(2, context.getStatistics().totalNumberOfIndices);
      assertEquals(2, context.getStatistics().wroteIndexes);

      /*
       *  Testing built OrientDB schema
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      OrientVertexType authorVertexType =  orientGraph.getVertexType("BookAuthor");
      OrientVertexType bookVertexType = orientGraph.getVertexType("Book");
      OrientEdgeType authorEdgeType = orientGraph.getEdgeType("HasAuthor");

      // vertices check
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertNotNull(authorVertexType.getProperty("id"));
      assertEquals("id", authorVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, authorVertexType.getProperty("id").getType());

      assertNotNull(authorVertexType.getProperty("name"));
      assertEquals("name", authorVertexType.getProperty("name").getName());
      assertEquals(OType.STRING, authorVertexType.getProperty("name").getType());

      assertNotNull(authorVertexType.getProperty("age"));
      assertEquals("age", authorVertexType.getProperty("age").getName());
      assertEquals(OType.INTEGER, authorVertexType.getProperty("age").getType());

      assertNotNull(bookVertexType.getProperty("id"));
      assertEquals("id", bookVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("id").getType());

      assertNotNull(bookVertexType.getProperty("title"));
      assertEquals("title", bookVertexType.getProperty("title").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("title").getType());

      assertNotNull(bookVertexType.getProperty("authorId"));
      assertEquals("authorId", bookVertexType.getProperty("authorId").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("authorId").getType());

      // edges check
      assertNotNull(authorEdgeType);

      assertEquals("HasAuthor", authorEdgeType.getName());

      // Indices check
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("BookAuthor.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("BookAuthor", "id"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Book.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Book", "id"));


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
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }


  @Test

  /*
   *  Three tables and two relationships with two different simple primary keys imported .
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String authorTableBuilding = "create memory table AUTHOR (ID varchar(256) not null,"+
          " NAME varchar(256) not null, AGE integer not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(authorTableBuilding);

      String bookTableBuilding = "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256),"+
          " AUTHOR_ID varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references AUTHOR(ID))";
      st.execute(bookTableBuilding);

      String itemTableBuilding = "create memory table ITEM (ID varchar(256) not null, BOOK_ID  varchar(256),"+
          " PRICE varchar(256) not null, primary key (ID), foreign key (BOOK_ID) references BOOK(ID))";
      st.execute(itemTableBuilding);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null, null);
      mapper.buildSourceDatabaseSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfVertexTypes);
      assertEquals(3, context.getStatistics().wroteVertexType);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().wroteEdgeType);
      assertEquals(3, context.getStatistics().totalNumberOfIndices);
      assertEquals(3, context.getStatistics().wroteIndexes);

      /*
       *  Testing built OrientDB schema
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      OrientVertexType authorVertexType =  orientGraph.getVertexType("Author");
      OrientVertexType bookVertexType = orientGraph.getVertexType("Book");
      OrientVertexType itemVertexType = orientGraph.getVertexType("Item");
      OrientEdgeType authorEdgeType = orientGraph.getEdgeType("HasAuthor");
      OrientEdgeType bookEdgeType = orientGraph.getEdgeType("HasBook");


      // vertices check
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertNotNull(authorVertexType.getProperty("id"));
      assertEquals("id", authorVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, authorVertexType.getProperty("id").getType());

      assertNotNull(authorVertexType.getProperty("name"));
      assertEquals("name", authorVertexType.getProperty("name").getName());
      assertEquals(OType.STRING, authorVertexType.getProperty("name").getType());

      assertNotNull(authorVertexType.getProperty("age"));
      assertEquals("age", authorVertexType.getProperty("age").getName());
      assertEquals(OType.INTEGER, authorVertexType.getProperty("age").getType());

      assertNotNull(bookVertexType.getProperty("id"));
      assertEquals("id", bookVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("id").getType());

      assertNotNull(bookVertexType.getProperty("title"));
      assertEquals("title", bookVertexType.getProperty("title").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("title").getType());

      assertNotNull(bookVertexType.getProperty("authorId"));
      assertEquals("authorId", bookVertexType.getProperty("authorId").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("authorId").getType());

      assertNotNull(itemVertexType.getProperty("id"));
      assertEquals("id", itemVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, itemVertexType.getProperty("id").getType());

      assertNotNull(itemVertexType.getProperty("bookId"));
      assertEquals("bookId", itemVertexType.getProperty("bookId").getName());
      assertEquals(OType.STRING, itemVertexType.getProperty("bookId").getType());

      assertNotNull(itemVertexType.getProperty("price"));
      assertEquals("price", itemVertexType.getProperty("price").getName());
      assertEquals(OType.STRING, itemVertexType.getProperty("price").getType());


      // edges check
      assertNotNull(authorEdgeType);
      assertNotNull(bookEdgeType);

      assertEquals("HasAuthor", authorEdgeType.getName());
      assertEquals("HasBook", bookEdgeType.getName());

      // Indices check
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Author.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Author", "id"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Book.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Book", "id"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Item.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Item", "id"));


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
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }


  @Test

  /*
   *  Three tables and two relationships with a simple primary keys twice imported.
   */

  public void test3() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String authorTableBuilding = "create memory table AUTHOR (ID varchar(256) not null,"+
          " NAME varchar(256) not null, AGE integer not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(authorTableBuilding);

      String bookTableBuilding = "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256),"+
          " AUTHOR_ID varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references AUTHOR(ID))";
      st.execute(bookTableBuilding);

      String articleTableBuilding = "create memory table ARTICLE (ID varchar(256) not null, TITLE  varchar(256),"+
          " DATE  date, AUTHOR_ID varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references AUTHOR(ID))";
      st.execute(articleTableBuilding);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null, null);
      mapper.buildSourceDatabaseSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfVertexTypes);
      assertEquals(3, context.getStatistics().wroteVertexType);
      assertEquals(1, context.getStatistics().totalNumberOfEdgeTypes);
      assertEquals(1, context.getStatistics().wroteEdgeType);
      assertEquals(3, context.getStatistics().totalNumberOfIndices);
      assertEquals(3, context.getStatistics().wroteIndexes);

      /*
       *  Testing built OrientDB schema
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      OrientVertexType authorVertexType =  orientGraph.getVertexType("Author");
      OrientVertexType bookVertexType = orientGraph.getVertexType("Book");
      OrientVertexType articleVertexType = orientGraph.getVertexType("Article");
      OrientEdgeType authorEdgeType = orientGraph.getEdgeType("HasAuthor");


      // vertices check
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertNotNull(authorVertexType.getProperty("id"));
      assertEquals("id", authorVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, authorVertexType.getProperty("id").getType());

      assertNotNull(authorVertexType.getProperty("name"));
      assertEquals("name", authorVertexType.getProperty("name").getName());
      assertEquals(OType.STRING, authorVertexType.getProperty("name").getType());

      assertNotNull(authorVertexType.getProperty("age"));
      assertEquals("age", authorVertexType.getProperty("age").getName());
      assertEquals(OType.INTEGER, authorVertexType.getProperty("age").getType());

      assertNotNull(bookVertexType.getProperty("id"));
      assertEquals("id", bookVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("id").getType());

      assertNotNull(bookVertexType.getProperty("title"));
      assertEquals("title", bookVertexType.getProperty("title").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("title").getType());

      assertNotNull(bookVertexType.getProperty("authorId"));
      assertEquals("authorId", bookVertexType.getProperty("authorId").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("authorId").getType());

      assertNotNull(articleVertexType.getProperty("id"));
      assertEquals("id", articleVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, articleVertexType.getProperty("id").getType());

      assertNotNull(articleVertexType.getProperty("title"));
      assertEquals("title", articleVertexType.getProperty("title").getName());
      assertEquals(OType.STRING, articleVertexType.getProperty("title").getType());

      assertNotNull(articleVertexType.getProperty("date"));
      assertEquals("date", articleVertexType.getProperty("date").getName());
      assertEquals(OType.DATE, articleVertexType.getProperty("date").getType());

      assertNotNull(articleVertexType.getProperty("authorId"));
      assertEquals("authorId", articleVertexType.getProperty("authorId").getName());
      assertEquals(OType.STRING, articleVertexType.getProperty("authorId").getType());

      // edges check
      assertNotNull(authorEdgeType);

      assertEquals("HasAuthor", authorEdgeType.getName());

      // Indices check
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Author.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Author", "id"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Book.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Book", "id"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Article.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Article", "id"));


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
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }



  @Test

  /*
   *  Two tables Foreign and Parent with a composite primary key imported from the parent table.
   */

  public void test4() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String authorTableBuilding = "create memory table AUTHOR (NAME varchar(256) not null," + 
          " SURNAME varchar(256) not null, AGE integer, primary key (NAME,SURNAME))";
      st = connection.createStatement();
      st.execute(authorTableBuilding);

      String bookTableBuilding = "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256),"+
          " AUTHOR_NAME varchar(256) not null, AUTHOR_SURNAME varchar(256) not null, primary key (ID)," + 
          " foreign key (AUTHOR_NAME,AUTHOR_SURNAME) references AUTHOR(NAME,SURNAME))";
      st.execute(bookTableBuilding);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null, null);
      mapper.buildSourceDatabaseSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfVertexTypes);
      assertEquals(2, context.getStatistics().wroteVertexType);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().wroteEdgeType);
      assertEquals(2, context.getStatistics().totalNumberOfIndices);
      assertEquals(2, context.getStatistics().wroteIndexes);

      /*
       *  Testing built OrientDB schema
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      OrientVertexType authorVertexType =  orientGraph.getVertexType("Author");
      OrientVertexType bookVertexType = orientGraph.getVertexType("Book");
      OrientEdgeType authorEdgeType = orientGraph.getEdgeType("Book2Author");

      // vertices check
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertNotNull(authorVertexType.getProperty("name"));
      assertEquals("name", authorVertexType.getProperty("name").getName());
      assertEquals(OType.STRING, authorVertexType.getProperty("name").getType());

      assertNotNull(authorVertexType.getProperty("surname"));
      assertEquals("surname", authorVertexType.getProperty("surname").getName());
      assertEquals(OType.STRING, authorVertexType.getProperty("surname").getType());

      assertNotNull(authorVertexType.getProperty("age"));
      assertEquals("age", authorVertexType.getProperty("age").getName());
      assertEquals(OType.INTEGER, authorVertexType.getProperty("age").getType());

      assertNotNull(bookVertexType.getProperty("id"));
      assertEquals("id", bookVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("id").getType());

      assertNotNull(bookVertexType.getProperty("title"));
      assertEquals("title", bookVertexType.getProperty("title").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("title").getType());

      assertNotNull(bookVertexType.getProperty("authorName"));
      assertEquals("authorName", bookVertexType.getProperty("authorName").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("authorName").getType());

      assertNotNull(bookVertexType.getProperty("authorSurname"));
      assertEquals("authorSurname", bookVertexType.getProperty("authorSurname").getName());
      assertEquals(OType.STRING, bookVertexType.getProperty("authorSurname").getType());

      // edges check
      assertNotNull(authorEdgeType);

      assertEquals("Book2Author", authorEdgeType.getName());

      // Indices check
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Author.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Author", "name", "surname"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Book.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Book", "id"));


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
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }

  @Test

  /*
   *  Three tables: 2 Parent and 1 join table which imports two different simple primary key.
   */

  public void test5() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

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

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null, null);
      mapper.buildSourceDatabaseSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfVertexTypes);
      assertEquals(3, context.getStatistics().wroteVertexType);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().wroteEdgeType);
      assertEquals(3, context.getStatistics().totalNumberOfIndices);
      assertEquals(3, context.getStatistics().wroteIndexes);

      /*
       *  Testing built OrientDB schema
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      OrientVertexType actorVertexType =  orientGraph.getVertexType("Actor");
      OrientVertexType filmVertexType = orientGraph.getVertexType("Film");
      OrientVertexType film2actorVertexType = orientGraph.getVertexType("FilmActor");
      OrientEdgeType actorEdgeType = orientGraph.getEdgeType("HasActor");
      OrientEdgeType filmEdgeType = orientGraph.getEdgeType("HasFilm");


      // vertices check
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNotNull(film2actorVertexType);

      // properties check
      assertNotNull(actorVertexType.getProperty("id"));
      assertEquals("id", actorVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, actorVertexType.getProperty("id").getType());

      assertNotNull(actorVertexType.getProperty("name"));
      assertEquals("name", actorVertexType.getProperty("name").getName());
      assertEquals(OType.STRING, actorVertexType.getProperty("name").getType());

      assertNotNull(actorVertexType.getProperty("surname"));
      assertEquals("surname", actorVertexType.getProperty("surname").getName());
      assertEquals(OType.STRING, actorVertexType.getProperty("surname").getType());


      assertNotNull(filmVertexType.getProperty("id"));
      assertEquals("id", filmVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, filmVertexType.getProperty("id").getType());

      assertNotNull(filmVertexType.getProperty("title"));
      assertEquals("title", filmVertexType.getProperty("title").getName());
      assertEquals(OType.STRING, filmVertexType.getProperty("title").getType());

      assertNotNull(filmVertexType.getProperty("year"));
      assertEquals("year", filmVertexType.getProperty("year").getName());
      assertEquals(OType.DATE, filmVertexType.getProperty("year").getType());

      assertNotNull(film2actorVertexType.getProperty("filmId"));
      assertEquals("filmId", film2actorVertexType.getProperty("filmId").getName());
      assertEquals(OType.STRING, film2actorVertexType.getProperty("filmId").getType());

      assertNotNull(film2actorVertexType.getProperty("actorId"));
      assertEquals("actorId", film2actorVertexType.getProperty("actorId").getName());
      assertEquals(OType.STRING, film2actorVertexType.getProperty("actorId").getType());

      // edges check
      assertNotNull(filmEdgeType);
      assertNotNull(actorEdgeType);

      assertEquals("HasFilm", filmEdgeType.getName());
      assertEquals("HasActor", actorEdgeType.getName());

      // Indices check
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Actor.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Actor", "id"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Film.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Film", "id"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("FilmActor.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("FilmActor", "filmId", "actorId"));


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
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }

  @Test

  /*
   *  Two tables: 1 Foreign and 1 Parent (parent has an inner referential integrity).
   *  The primary key is imported both by the foreign table and the attribute of the parent table itself.
   */

  public void test6() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"+
          " MGR_ID varchar(256) not null, NAME varchar(256) not null, primary key (EMP_ID), " + 
          " foreign key (MGR_ID) references EMPLOYEE(EMP_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table PROJECT (ID  varchar(256),"+
          " TITLE varchar(256) not null, PROJECT_MANAGER varchar(256) not null, primary key (ID)," +
          " foreign key (PROJECT_MANAGER) references EMPLOYEE(EMP_ID))";
      st.execute(foreignTableBuilding);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null, null);
      mapper.buildSourceDatabaseSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfVertexTypes);
      assertEquals(2, context.getStatistics().wroteVertexType);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().wroteEdgeType);
      assertEquals(2, context.getStatistics().totalNumberOfIndices);
      assertEquals(2, context.getStatistics().wroteIndexes);

      /*
       *  Testing built OrientDB schema
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      OrientVertexType employeeVertexType =  orientGraph.getVertexType("Employee");
      OrientVertexType projectVertexType = orientGraph.getVertexType("Project");
      OrientEdgeType projectManagerEdgeType = orientGraph.getEdgeType("HasProjectManager");
      OrientEdgeType mgrEdgeType = orientGraph.getEdgeType("HasMgr");


      // vertices check
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);

      // properties check
      assertNotNull(employeeVertexType.getProperty("empId"));
      assertEquals("empId", employeeVertexType.getProperty("empId").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("empId").getType());

      assertNotNull(employeeVertexType.getProperty("mgrId"));
      assertEquals("mgrId", employeeVertexType.getProperty("mgrId").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("mgrId").getType());

      assertNotNull(employeeVertexType.getProperty("name"));
      assertEquals("name", employeeVertexType.getProperty("name").getName());
      assertEquals(OType.STRING, employeeVertexType.getProperty("name").getType());

      assertNotNull(projectVertexType.getProperty("id"));
      assertEquals("id", projectVertexType.getProperty("id").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("id").getType());

      assertNotNull(projectVertexType.getProperty("title"));
      assertEquals("title", projectVertexType.getProperty("title").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("title").getType());

      assertNotNull(projectVertexType.getProperty("projectManager"));
      assertEquals("projectManager", projectVertexType.getProperty("projectManager").getName());
      assertEquals(OType.STRING, projectVertexType.getProperty("projectManager").getType());

      // edges check
      assertNotNull(mgrEdgeType);
      assertNotNull(projectManagerEdgeType);

      assertEquals("HasMgr", mgrEdgeType.getName());
      assertEquals("HasProjectManager", projectManagerEdgeType.getName());

      // Indices check
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Employee.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Employee", "empId"));

      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex("Project.pkey"));
      assertEquals(true, orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed("Project", "id"));


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
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }


  @Test

  /*
   * CheckAndUpdate OrientDB Schema
   */

  public void test7() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String authorTableBuilding = "create memory table AUTHOR (ID varchar(256) not null,"+
          " NAME varchar(256) not null, AGE integer not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(authorTableBuilding);

      String bookTableBuilding = "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256),"+
          " AUTHOR_ID varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references AUTHOR(ID))";
      st.execute(bookTableBuilding);

      String articleTableBuilding = "create memory table ARTICLE (ID varchar(256) not null, TITLE  varchar(256),"+
          " DATE  date, AUTHOR_ID varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references AUTHOR(ID))";
      st.execute(articleTableBuilding);

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null, null);
      mapper.buildSourceDatabaseSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);


      // dropping property from OrientDB Schema (from Author)
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);
      OrientVertexType authorVertexType =  orientGraph.getVertexType("Author");

      authorVertexType.createProperty("surname", OType.STRING);
      authorVertexType =  orientGraph.getVertexType("Author");
      assertEquals(4, authorVertexType.properties().size());
      Iterator<OProperty> it = authorVertexType.properties().iterator();

      List<String> props = new LinkedList<String>();
      while (it.hasNext()) {
        props.add(it.next().getName());
      }
      assertEquals(4, props.size());
      assertEquals(true, props.contains("id"));
      assertEquals(true, props.contains("age"));
      assertEquals(true, props.contains("name"));
      assertEquals(true, props.contains("surname"));

      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);
      authorVertexType =  orientGraph.getVertexType("Author");
      assertEquals(3, authorVertexType.properties().size());
      it = authorVertexType.properties().iterator();

      props.clear();
      while (it.hasNext()) {
        props.add(it.next().getName());
      }
      assertEquals(3, props.size());
      assertEquals(true, props.contains("id"));
      assertEquals(true, props.contains("age"));
      assertEquals(true, props.contains("name"));

      // dropping property from Graph Model (from Book)
      OrientVertexType articleVertexType = orientGraph.getVertexType("Article");

      mapper.getGraphModel().getVertexTypeByName("Article").removePropertyByName("title");
      assertEquals(3, mapper.getGraphModel().getVertexTypeByName("Article").getProperties().size());
      assertEquals("id", mapper.getGraphModel().getVertexTypeByName("Article").getProperties().get(0).getName());
      assertEquals("date", mapper.getGraphModel().getVertexTypeByName("Article").getProperties().get(1).getName());
      assertEquals("authorId", mapper.getGraphModel().getVertexTypeByName("Article").getProperties().get(2).getName());

      assertEquals(4, articleVertexType.properties().size());
      it = articleVertexType.properties().iterator();

      props.clear();
      while (it.hasNext()) {
        props.add(it.next().getName());
      }
      assertEquals(4, props.size());
      assertEquals(true, props.contains("id"));
      assertEquals(true, props.contains("title"));
      assertEquals(true, props.contains("authorId"));
      assertEquals(true, props.contains("date"));

      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);
      assertEquals(3, articleVertexType.properties().size());
      articleVertexType = orientGraph.getVertexType("Article");
      it = articleVertexType.properties().iterator();

      props.clear();
      while (it.hasNext()) {
        props.add(it.next().getName());
      }
      assertEquals(3, props.size());
      assertEquals(true, props.contains("id"));
      assertEquals(true, props.contains("authorId"));
      assertEquals(true, props.contains("date"));

      // adding a property to OrientDB Schema (to HasAuthor)
      OrientEdgeType authorEdgeType = orientGraph.getEdgeType("HasAuthor");
      authorEdgeType.createProperty("date", OType.DATE);
      assertEquals(1, authorEdgeType.properties().size());
      it = authorEdgeType.properties().iterator();
      assertEquals("date", it.next().getName());
      assertFalse(it.hasNext());

      modelWriter.writeModelOnOrient(mapper.getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);
      authorEdgeType = orientGraph.getEdgeType("HasAuthor");
      assertEquals(0, authorEdgeType.properties().size());


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
      if(orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }

}
