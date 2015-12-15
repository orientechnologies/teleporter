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

package com.orientechnologies.teleporter.test.rdbms.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OGraphModelBuildingTestCase {

  private OER2GraphMapper mapper;
  private OTeleporterContext context;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setQueryQuoteType("\"");
  }


  @Test

  /*
   *  Two tables Foreign and Parent with a simple primary key imported from the parent table.
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String parentTableBuilding = "create memory table BOOK_AUTHOR (ID varchar(256) not null,"+
          " NAME varchar(256) not null, AGE integer not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);


      String foreignTableBuilding = "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256),"+
          " AUTHOR_ID varchar(256) not null, primary key (ID), foreign key (AUTHOR_ID) references BOOK_AUTHOR(ID))";
      st.execute(foreignTableBuilding);

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().analizedRelationships);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType authorVertexType = mapper.getGraphModel().getVertexByName("BookAuthor");
      OVertexType bookVertexType = mapper.getGraphModel().getVertexByName("Book");
      OEdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasAuthor");

      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertEquals(3, authorVertexType.getProperties().size());

      assertNotNull(authorVertexType.getPropertyByName("id"));
      assertEquals("id", authorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, authorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("name"));
      assertEquals("name", authorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(2, authorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("age"));
      assertEquals("age", authorVertexType.getPropertyByName("age").getName());
      assertEquals("INTEGER", authorVertexType.getPropertyByName("age").getPropertyType());
      assertEquals(3, authorVertexType.getPropertyByName("age").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("age").isFromPrimaryKey());

      assertEquals(3, bookVertexType.getProperties().size());

      assertNotNull(bookVertexType.getPropertyByName("id"));
      assertEquals("id", bookVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, bookVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, bookVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("title"));
      assertEquals("title", bookVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("title").getPropertyType());
      assertEquals(2, bookVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorId"));
      assertEquals("authorId", bookVertexType.getPropertyByName("authorId").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorId").getPropertyType());
      assertEquals(3, bookVertexType.getPropertyByName("authorId").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorId").isFromPrimaryKey());

      // edges check
      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(authorEdgeType);

      assertEquals("HasAuthor", authorEdgeType.getName());
      assertEquals(0, authorEdgeType.getProperties().size());
      assertEquals("BookAuthor", authorEdgeType.getInVertexType().getName());


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
   *  Three tables and two relationships with two different simple primary keys imported .
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;
    
    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

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
      OVertexType authorVertexType = mapper.getGraphModel().getVertexByName("Author");
      OVertexType bookVertexType = mapper.getGraphModel().getVertexByName("Book");
      OVertexType itemVertexType = mapper.getGraphModel().getVertexByName("Item");
      OEdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasAuthor");
      OEdgeType bookEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasBook");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertEquals(3, authorVertexType.getProperties().size());

      assertNotNull(authorVertexType.getPropertyByName("id"));
      assertEquals("id", authorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, authorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("name"));
      assertEquals("name", authorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(2, authorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("age"));
      assertEquals("age", authorVertexType.getPropertyByName("age").getName());
      assertEquals("INTEGER", authorVertexType.getPropertyByName("age").getPropertyType());
      assertEquals(3, authorVertexType.getPropertyByName("age").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("age").isFromPrimaryKey());

      assertEquals(3, bookVertexType.getProperties().size());

      assertNotNull(bookVertexType.getPropertyByName("id"));
      assertEquals("id", bookVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, bookVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, bookVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("title"));
      assertEquals("title", bookVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("title").getPropertyType());
      assertEquals(2, bookVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorId"));
      assertEquals("authorId", bookVertexType.getPropertyByName("authorId").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorId").getPropertyType());
      assertEquals(3, bookVertexType.getPropertyByName("authorId").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorId").isFromPrimaryKey());

      assertEquals(3, itemVertexType.getProperties().size());

      assertNotNull(itemVertexType.getPropertyByName("id"));
      assertEquals("id", itemVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", itemVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, itemVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, itemVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(itemVertexType.getPropertyByName("bookId"));
      assertEquals("bookId", itemVertexType.getPropertyByName("bookId").getName());
      assertEquals("VARCHAR", itemVertexType.getPropertyByName("bookId").getPropertyType());
      assertEquals(2, itemVertexType.getPropertyByName("bookId").getOrdinalPosition());
      assertEquals(false, itemVertexType.getPropertyByName("bookId").isFromPrimaryKey());

      assertNotNull(itemVertexType.getPropertyByName("price"));
      assertEquals("price", itemVertexType.getPropertyByName("price").getName());
      assertEquals("VARCHAR", itemVertexType.getPropertyByName("price").getPropertyType());
      assertEquals(3, itemVertexType.getPropertyByName("price").getOrdinalPosition());
      assertEquals(false, itemVertexType.getPropertyByName("price").isFromPrimaryKey());


      // edges check
      assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(authorEdgeType);
      assertNotNull(bookEdgeType);

      assertEquals("HasAuthor", authorEdgeType.getName());
      assertEquals(0, authorEdgeType.getProperties().size());
      assertEquals("Author", authorEdgeType.getInVertexType().getName());

      assertEquals("HasBook", bookEdgeType.getName());
      assertEquals(0, bookEdgeType.getProperties().size());
      assertEquals("Book", bookEdgeType.getInVertexType().getName());


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
   *  Three tables and two relationships with a simple primary keys twice imported.
   */

  public void test3() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

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

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);  
      assertEquals(2, context.getStatistics().analizedRelationships);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType authorVertexType = mapper.getGraphModel().getVertexByName("Author");
      OVertexType bookVertexType = mapper.getGraphModel().getVertexByName("Book");
      OVertexType articleVertexType = mapper.getGraphModel().getVertexByName("Article");
      OEdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasAuthor");

      // vertices check
      assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertEquals(3, authorVertexType.getProperties().size());

      assertNotNull(authorVertexType.getPropertyByName("id"));
      assertEquals("id", authorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, authorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("name"));
      assertEquals("name", authorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(2, authorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("age"));
      assertEquals("age", authorVertexType.getPropertyByName("age").getName());
      assertEquals("INTEGER", authorVertexType.getPropertyByName("age").getPropertyType());
      assertEquals(3, authorVertexType.getPropertyByName("age").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("age").isFromPrimaryKey());

      assertEquals(3, bookVertexType.getProperties().size());

      assertNotNull(bookVertexType.getPropertyByName("id"));
      assertEquals("id", bookVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, bookVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, bookVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("title"));
      assertEquals("title", bookVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("title").getPropertyType());
      assertEquals(2, bookVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorId"));
      assertEquals("authorId", bookVertexType.getPropertyByName("authorId").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorId").getPropertyType());
      assertEquals(3, bookVertexType.getPropertyByName("authorId").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorId").isFromPrimaryKey());

      assertEquals(4, articleVertexType.getProperties().size());

      assertNotNull(articleVertexType.getPropertyByName("id"));
      assertEquals("id", articleVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", articleVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, articleVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, articleVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(articleVertexType.getPropertyByName("title"));
      assertEquals("title", articleVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", articleVertexType.getPropertyByName("title").getPropertyType());
      assertEquals(2, articleVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, articleVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(articleVertexType.getPropertyByName("date"));
      assertEquals("date", articleVertexType.getPropertyByName("date").getName());
      assertEquals("DATE", articleVertexType.getPropertyByName("date").getPropertyType());
      assertEquals(3, articleVertexType.getPropertyByName("date").getOrdinalPosition());
      assertEquals(false, articleVertexType.getPropertyByName("date").isFromPrimaryKey());

      assertNotNull(articleVertexType.getPropertyByName("authorId"));
      assertEquals("authorId", articleVertexType.getPropertyByName("authorId").getName());
      assertEquals("VARCHAR", articleVertexType.getPropertyByName("authorId").getPropertyType());
      assertEquals(4, articleVertexType.getPropertyByName("authorId").getOrdinalPosition());
      assertEquals(false, articleVertexType.getPropertyByName("authorId").isFromPrimaryKey());

      // edges check
      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(authorEdgeType);

      assertEquals("HasAuthor", authorEdgeType.getName());
      assertEquals(0, authorEdgeType.getProperties().size());
      assertEquals("Author", authorEdgeType.getInVertexType().getName());


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
   *  Two tables Foreign and Parent with a composite primary key imported from the parent table.
   */

  public void test4() {

    Connection connection = null;
    Statement st = null;
    
    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String authorTableBuilding = "create memory table AUTHOR (NAME varchar(256) not null," + 
          " SURNAME varchar(256) not null, AGE integer, primary key (NAME,SURNAME))";
      st = connection.createStatement();
      st.execute(authorTableBuilding);

      String bookTableBuilding = "create memory table BOOK (ID varchar(256) not null, TITLE  varchar(256),"+
          " AUTHOR_NAME varchar(256) not null, AUTHOR_SURNAME varchar(256) not null, primary key (ID)," + 
          " foreign key (AUTHOR_NAME,AUTHOR_SURNAME) references AUTHOR(NAME,SURNAME))";
      st.execute(bookTableBuilding);

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", null, null);
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);  
      assertEquals(1, context.getStatistics().analizedRelationships);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType authorVertexType = mapper.getGraphModel().getVertexByName("Author");
      OVertexType bookVertexType = mapper.getGraphModel().getVertexByName("Book");
      OEdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("Book2Author");

      // vertices check
      assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertEquals(3, authorVertexType.getProperties().size());

      assertNotNull(authorVertexType.getPropertyByName("name"));
      assertEquals("name", authorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("name").getPropertyType());
      assertEquals(1, authorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("surname"));
      assertEquals("surname", authorVertexType.getPropertyByName("surname").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("surname").getPropertyType());
      assertEquals(2, authorVertexType.getPropertyByName("surname").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("surname").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("age"));
      assertEquals("age", authorVertexType.getPropertyByName("age").getName());
      assertEquals("INTEGER", authorVertexType.getPropertyByName("age").getPropertyType());
      assertEquals(3, authorVertexType.getPropertyByName("age").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("age").isFromPrimaryKey());

      assertEquals(4, bookVertexType.getProperties().size());

      assertNotNull(bookVertexType.getPropertyByName("id"));
      assertEquals("id", bookVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("id").getPropertyType());
      assertEquals(1, bookVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, bookVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("title"));
      assertEquals("title", bookVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("title").getPropertyType());
      assertEquals(2, bookVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorName"));
      assertEquals("authorName", bookVertexType.getPropertyByName("authorName").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorName").getPropertyType());
      assertEquals(3, bookVertexType.getPropertyByName("authorName").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorName").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorSurname"));
      assertEquals("authorSurname", bookVertexType.getPropertyByName("authorSurname").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorSurname").getPropertyType());
      assertEquals(4, bookVertexType.getPropertyByName("authorSurname").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorSurname").isFromPrimaryKey());

      // edges check
      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(authorEdgeType);

      assertEquals("Book2Author", authorEdgeType.getName());
      assertEquals(0, authorEdgeType.getProperties().size());
      assertEquals("Author", authorEdgeType.getInVertexType().getName());


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

  public void test5() {

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

      assertEquals("HasActor", actorEdgeType.getName());
      assertEquals(0, actorEdgeType.getProperties().size());
      assertEquals("Actor", actorEdgeType.getInVertexType().getName());


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
   *  Two tables: 1 Foreign and 1 Parent (parent has an inner referential integrity).
   *  The primary key is imported both by the foreign table and the attribute of the parent table itself.
   */

  public void test6() {

    Connection connection = null;
    Statement st = null;
    
    try {

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String parentTableBuilding = "create memory table EMPLOYEE (EMP_ID varchar(256) not null,"+
          " MGR_ID varchar(256) not null, NAME varchar(256) not null, primary key (EMP_ID), " + 
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

      assertEquals("HasProjectManager", projectManagerEdgeType.getName());
      assertEquals(0, projectManagerEdgeType.getProperties().size());
      assertEquals("Employee", projectManagerEdgeType.getInVertexType().getName());


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
