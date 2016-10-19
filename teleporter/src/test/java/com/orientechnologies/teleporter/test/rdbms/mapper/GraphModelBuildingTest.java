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

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OClassMapper;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.ORelationship;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import org.junit.Assert;
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

public class GraphModelBuildingTest {

  private OER2GraphMapper    mapper;
  private OTeleporterContext context;
  private ODBQueryEngine dbQueryEngine;
  private String driver = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private OSourceDatabaseInfo sourceDBInfo;


  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance();
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.sourceDBInfo = new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
  }


  @Test

  /*
   *  Two tables Foreign and Parent with a simple primary key imported from the parent table.
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;

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

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType authorVertexType = mapper.getGraphModel().getVertexTypeByName("BookAuthor");
      OVertexType bookVertexType = mapper.getGraphModel().getVertexTypeByName("Book");
      OEdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasAuthor");

      // vertices check
      Assert.assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertEquals(3, authorVertexType.getProperties().size());

      assertNotNull(authorVertexType.getPropertyByName("id"));
      assertEquals("id", authorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, authorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("name"));
      assertEquals("name", authorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(2, authorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("age"));
      assertEquals("age", authorVertexType.getPropertyByName("age").getName());
      assertEquals("INTEGER", authorVertexType.getPropertyByName("age").getOriginalType());
      assertEquals(3, authorVertexType.getPropertyByName("age").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("age").isFromPrimaryKey());

      assertEquals(3, bookVertexType.getProperties().size());

      assertNotNull(bookVertexType.getPropertyByName("id"));
      assertEquals("id", bookVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, bookVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, bookVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("title"));
      assertEquals("title", bookVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, bookVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorId"));
      assertEquals("authorId", bookVertexType.getPropertyByName("authorId").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorId").getOriginalType());
      assertEquals(3, bookVertexType.getPropertyByName("authorId").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorId").isFromPrimaryKey());

      // edges check
      assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(authorEdgeType);

      assertEquals("HasAuthor", authorEdgeType.getName());
      assertEquals(0, authorEdgeType.getProperties().size());
      assertEquals("BookAuthor", authorEdgeType.getInVertexType().getName());
      assertEquals(1, authorEdgeType.getNumberRelationshipsRepresented());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(2, mapper.getVertexType2classMappers().size());
      assertEquals(2, mapper.getEntity2classMappers().size());

      OEntity bookEntity = mapper.getDataBaseSchema().getEntityByName("BOOK");
      assertEquals(1, mapper.getClassMappersByVertex(bookVertexType).size());
      OClassMapper bookClassMapper = mapper.getClassMappersByVertex(bookVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(bookEntity).size());
      assertEquals(bookClassMapper, mapper.getClassMappersByEntity(bookEntity).get(0));
      assertEquals(bookClassMapper.getEntity(), bookEntity);
      assertEquals(bookClassMapper.getVertexType(), bookVertexType);

      assertEquals(3, bookClassMapper.attribute2property.size());
      assertEquals(3, bookClassMapper.property2attribute.size());
      assertEquals("id", bookClassMapper.attribute2property.get("ID"));
      assertEquals("title", bookClassMapper.attribute2property.get("TITLE"));
      assertEquals("authorId", bookClassMapper.attribute2property.get("AUTHOR_ID"));
      assertEquals("ID", bookClassMapper.property2attribute.get("id"));
      assertEquals("TITLE", bookClassMapper.property2attribute.get("title"));
      assertEquals("AUTHOR_ID", bookClassMapper.property2attribute.get("authorId"));

      OEntity bookAuthorEntity = mapper.getDataBaseSchema().getEntityByName("BOOK_AUTHOR");
      assertEquals(1, mapper.getClassMappersByVertex(authorVertexType).size());
      OClassMapper bookAuthorClassMapper =  mapper.getClassMappersByVertex(authorVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(bookAuthorEntity).size());
      assertEquals(bookAuthorClassMapper, mapper.getClassMappersByEntity(bookAuthorEntity).get(0));
      assertEquals(bookAuthorClassMapper.getEntity(), bookAuthorEntity);
      assertEquals(bookAuthorClassMapper.getVertexType(), authorVertexType);

      assertEquals(3, bookAuthorClassMapper.attribute2property.size());
      assertEquals(3, bookAuthorClassMapper.property2attribute.size());
      assertEquals("id", bookAuthorClassMapper.attribute2property.get("ID"));
      assertEquals("name", bookAuthorClassMapper.attribute2property.get("NAME"));
      assertEquals("age", bookAuthorClassMapper.attribute2property.get("AGE"));
      assertEquals("ID", bookAuthorClassMapper.property2attribute.get("id"));
      assertEquals("NAME", bookAuthorClassMapper.property2attribute.get("name"));
      assertEquals("AGE", bookAuthorClassMapper.property2attribute.get("age"));

      // Relationships-Edges Mapping

      Iterator<ORelationship> it = bookEntity.getOutRelationships().iterator();
      ORelationship hasAuthorRelationship = it.next();
      assertFalse(it.hasNext());

      assertEquals(1, mapper.getRelationship2edgeType().size());
      assertEquals(authorEdgeType, mapper.getRelationship2edgeType().get(hasAuthorRelationship));

      assertEquals(1, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(authorEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(authorEdgeType).contains(hasAuthorRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

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
   *  Three tables and two relationships with two different simple primary keys imported .
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;

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

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType authorVertexType = mapper.getGraphModel().getVertexTypeByName("Author");
      OVertexType bookVertexType = mapper.getGraphModel().getVertexTypeByName("Book");
      OVertexType itemVertexType = mapper.getGraphModel().getVertexTypeByName("Item");
      OEdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasAuthor");
      OEdgeType bookEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasBook");

      // vertices check
      Assert.assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertEquals(3, authorVertexType.getProperties().size());

      assertNotNull(authorVertexType.getPropertyByName("id"));
      assertEquals("id", authorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, authorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("name"));
      assertEquals("name", authorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(2, authorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("age"));
      assertEquals("age", authorVertexType.getPropertyByName("age").getName());
      assertEquals("INTEGER", authorVertexType.getPropertyByName("age").getOriginalType());
      assertEquals(3, authorVertexType.getPropertyByName("age").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("age").isFromPrimaryKey());

      assertEquals(3, bookVertexType.getProperties().size());

      assertNotNull(bookVertexType.getPropertyByName("id"));
      assertEquals("id", bookVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, bookVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, bookVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("title"));
      assertEquals("title", bookVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, bookVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorId"));
      assertEquals("authorId", bookVertexType.getPropertyByName("authorId").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorId").getOriginalType());
      assertEquals(3, bookVertexType.getPropertyByName("authorId").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorId").isFromPrimaryKey());

      assertEquals(3, itemVertexType.getProperties().size());

      assertNotNull(itemVertexType.getPropertyByName("id"));
      assertEquals("id", itemVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", itemVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, itemVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, itemVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(itemVertexType.getPropertyByName("bookId"));
      assertEquals("bookId", itemVertexType.getPropertyByName("bookId").getName());
      assertEquals("VARCHAR", itemVertexType.getPropertyByName("bookId").getOriginalType());
      assertEquals(2, itemVertexType.getPropertyByName("bookId").getOrdinalPosition());
      assertEquals(false, itemVertexType.getPropertyByName("bookId").isFromPrimaryKey());

      assertNotNull(itemVertexType.getPropertyByName("price"));
      assertEquals("price", itemVertexType.getPropertyByName("price").getName());
      assertEquals("VARCHAR", itemVertexType.getPropertyByName("price").getOriginalType());
      assertEquals(3, itemVertexType.getPropertyByName("price").getOrdinalPosition());
      assertEquals(false, itemVertexType.getPropertyByName("price").isFromPrimaryKey());


      // edges check
      Assert.assertEquals(2, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(authorEdgeType);
      assertNotNull(bookEdgeType);

      assertEquals("HasAuthor", authorEdgeType.getName());
      assertEquals(0, authorEdgeType.getProperties().size());
      assertEquals("Author", authorEdgeType.getInVertexType().getName());
      assertEquals(1, authorEdgeType.getNumberRelationshipsRepresented());

      assertEquals("HasBook", bookEdgeType.getName());
      assertEquals(0, bookEdgeType.getProperties().size());
      assertEquals("Book", bookEdgeType.getInVertexType().getName());
      assertEquals(1, bookEdgeType.getNumberRelationshipsRepresented());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2classMappers().size());
      assertEquals(3, mapper.getEntity2classMappers().size());

      OEntity bookEntity = mapper.getDataBaseSchema().getEntityByName("BOOK");
      assertEquals(1, mapper.getClassMappersByVertex(bookVertexType).size());
      OClassMapper bookClassMapper = mapper.getClassMappersByVertex(bookVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(bookEntity).size());
      assertEquals(bookClassMapper, mapper.getClassMappersByEntity(bookEntity).get(0));
      assertEquals(bookClassMapper.getEntity(), bookEntity);
      assertEquals(bookClassMapper.getVertexType(), bookVertexType);

      assertEquals(3, bookClassMapper.attribute2property.size());
      assertEquals(3, bookClassMapper.property2attribute.size());
      assertEquals("id", bookClassMapper.attribute2property.get("ID"));
      assertEquals("title", bookClassMapper.attribute2property.get("TITLE"));
      assertEquals("authorId", bookClassMapper.attribute2property.get("AUTHOR_ID"));
      assertEquals("ID", bookClassMapper.property2attribute.get("id"));
      assertEquals("TITLE", bookClassMapper.property2attribute.get("title"));
      assertEquals("AUTHOR_ID", bookClassMapper.property2attribute.get("authorId"));

      OEntity authorEntity = mapper.getDataBaseSchema().getEntityByName("AUTHOR");
      assertEquals(1, mapper.getClassMappersByVertex(authorVertexType).size());
      OClassMapper authorClassMapper =  mapper.getClassMappersByVertex(authorVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(authorEntity).size());
      assertEquals(authorClassMapper, mapper.getClassMappersByEntity(authorEntity).get(0));
      assertEquals(authorClassMapper.getEntity(), authorEntity);
      assertEquals(authorClassMapper.getVertexType(), authorVertexType);

      assertEquals(3, authorClassMapper.attribute2property.size());
      assertEquals(3, authorClassMapper.property2attribute.size());
      assertEquals("id", authorClassMapper.attribute2property.get("ID"));
      assertEquals("name", authorClassMapper.attribute2property.get("NAME"));
      assertEquals("age", authorClassMapper.attribute2property.get("AGE"));
      assertEquals("ID", authorClassMapper.property2attribute.get("id"));
      assertEquals("NAME", authorClassMapper.property2attribute.get("name"));
      assertEquals("AGE", authorClassMapper.property2attribute.get("age"));

      OEntity itemEntity = mapper.getDataBaseSchema().getEntityByName("ITEM");
      assertEquals(1, mapper.getClassMappersByVertex(itemVertexType).size());
      OClassMapper itemClassMapper =  mapper.getClassMappersByVertex(itemVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(itemEntity).size());
      assertEquals(itemClassMapper, mapper.getClassMappersByEntity(itemEntity).get(0));
      assertEquals(itemClassMapper.getEntity(), itemEntity);
      assertEquals(itemClassMapper.getVertexType(), itemVertexType);

      assertEquals(3, itemClassMapper.attribute2property.size());
      assertEquals(3, itemClassMapper.property2attribute.size());
      assertEquals("id", itemClassMapper.attribute2property.get("ID"));
      assertEquals("bookId", itemClassMapper.attribute2property.get("BOOK_ID"));
      assertEquals("price", itemClassMapper.attribute2property.get("PRICE"));
      assertEquals("ID", itemClassMapper.property2attribute.get("id"));
      assertEquals("BOOK_ID", itemClassMapper.property2attribute.get("bookId"));
      assertEquals("PRICE", itemClassMapper.property2attribute.get("price"));

      // Relationships-Edges Mapping

      Iterator<ORelationship> it = bookEntity.getOutRelationships().iterator();
      ORelationship hasAuthorRelationship = it.next();
      assertFalse(it.hasNext());
      it = itemEntity.getOutRelationships().iterator();
      ORelationship hasBookRelationship = it.next();
      assertFalse(it.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(authorEdgeType, mapper.getRelationship2edgeType().get(hasAuthorRelationship));
      assertEquals(bookEdgeType, mapper.getRelationship2edgeType().get(hasBookRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(authorEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(authorEdgeType).contains(hasAuthorRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(bookEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(bookEdgeType).contains(hasBookRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

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
   *  Three tables and two relationships with a simple primary keys twice imported.
   */

  public void test3() {

    Connection connection = null;
    Statement st = null;

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

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType authorVertexType = mapper.getGraphModel().getVertexTypeByName("Author");
      OVertexType bookVertexType = mapper.getGraphModel().getVertexTypeByName("Book");
      OVertexType articleVertexType = mapper.getGraphModel().getVertexTypeByName("Article");
      OEdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasAuthor");

      // vertices check
      Assert.assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertEquals(3, authorVertexType.getProperties().size());

      assertNotNull(authorVertexType.getPropertyByName("id"));
      assertEquals("id", authorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, authorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("name"));
      assertEquals("name", authorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(2, authorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("age"));
      assertEquals("age", authorVertexType.getPropertyByName("age").getName());
      assertEquals("INTEGER", authorVertexType.getPropertyByName("age").getOriginalType());
      assertEquals(3, authorVertexType.getPropertyByName("age").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("age").isFromPrimaryKey());

      assertEquals(3, bookVertexType.getProperties().size());

      assertNotNull(bookVertexType.getPropertyByName("id"));
      assertEquals("id", bookVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, bookVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, bookVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("title"));
      assertEquals("title", bookVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, bookVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorId"));
      assertEquals("authorId", bookVertexType.getPropertyByName("authorId").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorId").getOriginalType());
      assertEquals(3, bookVertexType.getPropertyByName("authorId").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorId").isFromPrimaryKey());

      assertEquals(4, articleVertexType.getProperties().size());

      assertNotNull(articleVertexType.getPropertyByName("id"));
      assertEquals("id", articleVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", articleVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, articleVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, articleVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(articleVertexType.getPropertyByName("title"));
      assertEquals("title", articleVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", articleVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, articleVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, articleVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(articleVertexType.getPropertyByName("date"));
      assertEquals("date", articleVertexType.getPropertyByName("date").getName());
      assertEquals("DATE", articleVertexType.getPropertyByName("date").getOriginalType());
      assertEquals(3, articleVertexType.getPropertyByName("date").getOrdinalPosition());
      assertEquals(false, articleVertexType.getPropertyByName("date").isFromPrimaryKey());

      assertNotNull(articleVertexType.getPropertyByName("authorId"));
      assertEquals("authorId", articleVertexType.getPropertyByName("authorId").getName());
      assertEquals("VARCHAR", articleVertexType.getPropertyByName("authorId").getOriginalType());
      assertEquals(4, articleVertexType.getPropertyByName("authorId").getOrdinalPosition());
      assertEquals(false, articleVertexType.getPropertyByName("authorId").isFromPrimaryKey());

      // edges check
      Assert.assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(authorEdgeType);

      assertEquals("HasAuthor", authorEdgeType.getName());
      assertEquals(0, authorEdgeType.getProperties().size());
      assertEquals("Author", authorEdgeType.getInVertexType().getName());
      assertEquals(2, authorEdgeType.getNumberRelationshipsRepresented());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2classMappers().size());
      assertEquals(3, mapper.getEntity2classMappers().size());

      OEntity bookEntity = mapper.getDataBaseSchema().getEntityByName("BOOK");
      assertEquals(1, mapper.getClassMappersByVertex(bookVertexType).size());
      OClassMapper bookClassMapper = mapper.getClassMappersByVertex(bookVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(bookEntity).size());
      assertEquals(bookClassMapper, mapper.getClassMappersByEntity(bookEntity).get(0));
      assertEquals(bookClassMapper.getEntity(), bookEntity);
      assertEquals(bookClassMapper.getVertexType(), bookVertexType);

      assertEquals(3, bookClassMapper.attribute2property.size());
      assertEquals(3, bookClassMapper.property2attribute.size());
      assertEquals("id", bookClassMapper.attribute2property.get("ID"));
      assertEquals("title", bookClassMapper.attribute2property.get("TITLE"));
      assertEquals("authorId", bookClassMapper.attribute2property.get("AUTHOR_ID"));
      assertEquals("ID", bookClassMapper.property2attribute.get("id"));
      assertEquals("TITLE", bookClassMapper.property2attribute.get("title"));
      assertEquals("AUTHOR_ID", bookClassMapper.property2attribute.get("authorId"));

      OEntity authorEntity = mapper.getDataBaseSchema().getEntityByName("AUTHOR");
      assertEquals(1, mapper.getClassMappersByVertex(authorVertexType).size());
      OClassMapper authorClassMapper =  mapper.getClassMappersByVertex(authorVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(authorEntity).size());
      assertEquals(authorClassMapper, mapper.getClassMappersByEntity(authorEntity).get(0));
      assertEquals(authorClassMapper.getEntity(), authorEntity);
      assertEquals(authorClassMapper.getVertexType(), authorVertexType);

      assertEquals(3, authorClassMapper.attribute2property.size());
      assertEquals(3, authorClassMapper.property2attribute.size());
      assertEquals("id", authorClassMapper.attribute2property.get("ID"));
      assertEquals("name", authorClassMapper.attribute2property.get("NAME"));
      assertEquals("age", authorClassMapper.attribute2property.get("AGE"));
      assertEquals("ID", authorClassMapper.property2attribute.get("id"));
      assertEquals("NAME", authorClassMapper.property2attribute.get("name"));
      assertEquals("AGE", authorClassMapper.property2attribute.get("age"));

      OEntity articleEntity = mapper.getDataBaseSchema().getEntityByName("ARTICLE");
      assertEquals(1, mapper.getClassMappersByVertex(articleVertexType).size());
      OClassMapper articleClassMapper =  mapper.getClassMappersByVertex(articleVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(articleEntity).size());
      assertEquals(articleClassMapper, mapper.getClassMappersByEntity(articleEntity).get(0));
      assertEquals(articleClassMapper.getEntity(), articleEntity);
      assertEquals(articleClassMapper.getVertexType(), articleVertexType);

      assertEquals(4, articleClassMapper.attribute2property.size());
      assertEquals(4, articleClassMapper.property2attribute.size());
      assertEquals("id", articleClassMapper.attribute2property.get("ID"));
      assertEquals("title", articleClassMapper.attribute2property.get("TITLE"));
      assertEquals("date", articleClassMapper.attribute2property.get("DATE"));
      assertEquals("authorId", articleClassMapper.attribute2property.get("AUTHOR_ID"));
      assertEquals("ID", articleClassMapper.property2attribute.get("id"));
      assertEquals("TITLE", articleClassMapper.property2attribute.get("title"));
      assertEquals("DATE", articleClassMapper.property2attribute.get("date"));
      assertEquals("AUTHOR_ID", articleClassMapper.property2attribute.get("authorId"));

      // Relationships-Edges Mapping

      Iterator<ORelationship> it = bookEntity.getOutRelationships().iterator();
      ORelationship hasAuthorRelationship1 = it.next();
      assertFalse(it.hasNext());
      it = articleEntity.getOutRelationships().iterator();
      ORelationship hasAuthorRelationship2 = it.next();
      assertFalse(it.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(authorEdgeType, mapper.getRelationship2edgeType().get(hasAuthorRelationship1));
      assertEquals(authorEdgeType, mapper.getRelationship2edgeType().get(hasAuthorRelationship2));

      assertEquals(1, mapper.getEdgeType2relationships().size());
      assertEquals(2, mapper.getEdgeType2relationships().get(authorEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(authorEdgeType).contains(hasAuthorRelationship1));
      assertTrue(mapper.getEdgeType2relationships().get(authorEdgeType).contains(hasAuthorRelationship2));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

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
   *  Two tables Foreign and Parent with a composite primary key imported from the parent table.
   */

  public void test4() {

    Connection connection = null;
    Statement st = null;

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

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(1, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(1, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType authorVertexType = mapper.getGraphModel().getVertexTypeByName("Author");
      OVertexType bookVertexType = mapper.getGraphModel().getVertexTypeByName("Book");
      OEdgeType authorEdgeType = mapper.getGraphModel().getEdgeTypeByName("Book2Author");

      // vertices check
      Assert.assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(authorVertexType);
      assertNotNull(bookVertexType);

      // properties check
      assertEquals(3, authorVertexType.getProperties().size());

      assertNotNull(authorVertexType.getPropertyByName("name"));
      assertEquals("name", authorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(1, authorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("surname"));
      assertEquals("surname", authorVertexType.getPropertyByName("surname").getName());
      assertEquals("VARCHAR", authorVertexType.getPropertyByName("surname").getOriginalType());
      assertEquals(2, authorVertexType.getPropertyByName("surname").getOrdinalPosition());
      assertEquals(true, authorVertexType.getPropertyByName("surname").isFromPrimaryKey());

      assertNotNull(authorVertexType.getPropertyByName("age"));
      assertEquals("age", authorVertexType.getPropertyByName("age").getName());
      assertEquals("INTEGER", authorVertexType.getPropertyByName("age").getOriginalType());
      assertEquals(3, authorVertexType.getPropertyByName("age").getOrdinalPosition());
      assertEquals(false, authorVertexType.getPropertyByName("age").isFromPrimaryKey());

      assertEquals(4, bookVertexType.getProperties().size());

      assertNotNull(bookVertexType.getPropertyByName("id"));
      assertEquals("id", bookVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, bookVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, bookVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("title"));
      assertEquals("title", bookVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, bookVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorName"));
      assertEquals("authorName", bookVertexType.getPropertyByName("authorName").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorName").getOriginalType());
      assertEquals(3, bookVertexType.getPropertyByName("authorName").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorName").isFromPrimaryKey());

      assertNotNull(bookVertexType.getPropertyByName("authorSurname"));
      assertEquals("authorSurname", bookVertexType.getPropertyByName("authorSurname").getName());
      assertEquals("VARCHAR", bookVertexType.getPropertyByName("authorSurname").getOriginalType());
      assertEquals(4, bookVertexType.getPropertyByName("authorSurname").getOrdinalPosition());
      assertEquals(false, bookVertexType.getPropertyByName("authorSurname").isFromPrimaryKey());

      // edges check
      Assert.assertEquals(1, mapper.getGraphModel().getEdgesType().size());
      assertNotNull(authorEdgeType);

      assertEquals("Book2Author", authorEdgeType.getName());
      assertEquals(0, authorEdgeType.getProperties().size());
      assertEquals("Author", authorEdgeType.getInVertexType().getName());
      assertEquals(1, authorEdgeType.getNumberRelationshipsRepresented());

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(2, mapper.getVertexType2classMappers().size());
      assertEquals(2, mapper.getEntity2classMappers().size());

      OEntity bookEntity = mapper.getDataBaseSchema().getEntityByName("BOOK");
      assertEquals(1, mapper.getClassMappersByVertex(bookVertexType).size());
      OClassMapper bookClassMapper = mapper.getClassMappersByVertex(bookVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(bookEntity).size());
      assertEquals(bookClassMapper, mapper.getClassMappersByEntity(bookEntity).get(0));
      assertEquals(bookClassMapper.getEntity(), bookEntity);
      assertEquals(bookClassMapper.getVertexType(), bookVertexType);

      assertEquals(4, bookClassMapper.attribute2property.size());
      assertEquals(4, bookClassMapper.property2attribute.size());
      assertEquals("id", bookClassMapper.attribute2property.get("ID"));
      assertEquals("title", bookClassMapper.attribute2property.get("TITLE"));
      assertEquals("authorName", bookClassMapper.attribute2property.get("AUTHOR_NAME"));
      assertEquals("authorSurname", bookClassMapper.attribute2property.get("AUTHOR_SURNAME"));
      assertEquals("ID", bookClassMapper.property2attribute.get("id"));
      assertEquals("TITLE", bookClassMapper.property2attribute.get("title"));
      assertEquals("AUTHOR_NAME", bookClassMapper.property2attribute.get("authorName"));
      assertEquals("AUTHOR_SURNAME", bookClassMapper.property2attribute.get("authorSurname"));

      OEntity authorEntity = mapper.getDataBaseSchema().getEntityByName("AUTHOR");
      assertEquals(1, mapper.getClassMappersByVertex(authorVertexType).size());
      OClassMapper authorClassMapper =  mapper.getClassMappersByVertex(authorVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(authorEntity).size());
      assertEquals(authorClassMapper, mapper.getClassMappersByEntity(authorEntity).get(0));
      assertEquals(authorClassMapper.getEntity(), authorEntity);
      assertEquals(authorClassMapper.getVertexType(), authorVertexType);

      assertEquals(3, authorClassMapper.attribute2property.size());
      assertEquals(3, authorClassMapper.property2attribute.size());
      assertEquals("name", authorClassMapper.attribute2property.get("NAME"));
      assertEquals("surname", authorClassMapper.attribute2property.get("SURNAME"));
      assertEquals("age", authorClassMapper.attribute2property.get("AGE"));
      assertEquals("NAME", authorClassMapper.property2attribute.get("name"));
      assertEquals("SURNAME", authorClassMapper.property2attribute.get("surname"));
      assertEquals("AGE", authorClassMapper.property2attribute.get("age"));

      // Relationships-Edges Mapping

      Iterator<ORelationship> it = bookEntity.getOutRelationships().iterator();
      ORelationship hasAuthorRelationship = it.next();
      assertFalse(it.hasNext());

      assertEquals(1, mapper.getRelationship2edgeType().size());
      assertEquals(authorEdgeType, mapper.getRelationship2edgeType().get(hasAuthorRelationship));

      assertEquals(1, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(authorEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(authorEdgeType).contains(hasAuthorRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

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
   *  Three tables: 2 Parent and 1 join table which imports two different simple primary key.
   */

  public void test5() {

    Connection connection = null;
    Statement st = null;

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

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(3, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType actorVertexType = mapper.getGraphModel().getVertexTypeByName("Actor");
      OVertexType filmVertexType = mapper.getGraphModel().getVertexTypeByName("Film");
      OVertexType film2actorVertexType = mapper.getGraphModel().getVertexTypeByName("FilmActor");
      OEdgeType actorEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasActor");
      OEdgeType filmEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasFilm");


      // vertices check
      Assert.assertEquals(3, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(actorVertexType);
      assertNotNull(filmVertexType);
      assertNotNull(film2actorVertexType);

      // properties check
      assertEquals(3, actorVertexType.getProperties().size());

      assertNotNull(actorVertexType.getPropertyByName("id"));
      assertEquals("id", actorVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, actorVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, actorVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("name"));
      assertEquals("name", actorVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(2, actorVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertNotNull(actorVertexType.getPropertyByName("surname"));
      assertEquals("surname", actorVertexType.getPropertyByName("surname").getName());
      assertEquals("VARCHAR", actorVertexType.getPropertyByName("surname").getOriginalType());
      assertEquals(3, actorVertexType.getPropertyByName("surname").getOrdinalPosition());
      assertEquals(false, actorVertexType.getPropertyByName("surname").isFromPrimaryKey());

      assertEquals(3, filmVertexType.getProperties().size());

      assertNotNull(filmVertexType.getPropertyByName("id"));
      assertEquals("id", filmVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, filmVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, filmVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("title"));
      assertEquals("title", filmVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", filmVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, filmVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(filmVertexType.getPropertyByName("year"));
      assertEquals("year", filmVertexType.getPropertyByName("year").getName());
      assertEquals("DATE", filmVertexType.getPropertyByName("year").getOriginalType());
      assertEquals(3, filmVertexType.getPropertyByName("year").getOrdinalPosition());
      assertEquals(false, filmVertexType.getPropertyByName("year").isFromPrimaryKey());

      assertEquals(2, film2actorVertexType.getProperties().size());

      assertNotNull(film2actorVertexType.getPropertyByName("filmId"));
      assertEquals("filmId", film2actorVertexType.getPropertyByName("filmId").getName());
      assertEquals("VARCHAR", film2actorVertexType.getPropertyByName("filmId").getOriginalType());
      assertEquals(1, film2actorVertexType.getPropertyByName("filmId").getOrdinalPosition());
      assertEquals(true, film2actorVertexType.getPropertyByName("filmId").isFromPrimaryKey());

      assertNotNull(film2actorVertexType.getPropertyByName("actorId"));
      assertEquals("actorId", film2actorVertexType.getPropertyByName("actorId").getName());
      assertEquals("VARCHAR", film2actorVertexType.getPropertyByName("actorId").getOriginalType());
      assertEquals(2, film2actorVertexType.getPropertyByName("actorId").getOrdinalPosition());
      assertEquals(true, film2actorVertexType.getPropertyByName("actorId").isFromPrimaryKey());

      // edges check
      Assert.assertEquals(2, mapper.getGraphModel().getEdgesType().size());
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

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(3, mapper.getVertexType2classMappers().size());
      assertEquals(3, mapper.getEntity2classMappers().size());

      OEntity filmEntity = mapper.getDataBaseSchema().getEntityByName("FILM");
      assertEquals(1, mapper.getClassMappersByVertex(filmVertexType).size());
      OClassMapper filmClassMapper = mapper.getClassMappersByVertex(filmVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(filmEntity).size());
      assertEquals(filmClassMapper, mapper.getClassMappersByEntity(filmEntity).get(0));
      assertEquals(filmClassMapper.getEntity(), filmEntity);
      assertEquals(filmClassMapper.getVertexType(), filmVertexType);

      assertEquals(3, filmClassMapper.attribute2property.size());
      assertEquals(3, filmClassMapper.property2attribute.size());
      assertEquals("id", filmClassMapper.attribute2property.get("ID"));
      assertEquals("title", filmClassMapper.attribute2property.get("TITLE"));
      assertEquals("year", filmClassMapper.attribute2property.get("YEAR"));
      assertEquals("ID", filmClassMapper.property2attribute.get("id"));
      assertEquals("TITLE", filmClassMapper.property2attribute.get("title"));
      assertEquals("YEAR", filmClassMapper.property2attribute.get("year"));

      OEntity actorEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR");
      assertEquals(1, mapper.getClassMappersByVertex(actorVertexType).size());
      OClassMapper actorClassMapper =  mapper.getClassMappersByVertex(actorVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(actorEntity).size());
      assertEquals(actorClassMapper, mapper.getClassMappersByEntity(actorEntity).get(0));
      assertEquals(actorClassMapper.getEntity(), actorEntity);
      assertEquals(actorClassMapper.getVertexType(), actorVertexType);

      assertEquals(3, actorClassMapper.attribute2property.size());
      assertEquals(3, actorClassMapper.property2attribute.size());
      assertEquals("id", actorClassMapper.attribute2property.get("ID"));
      assertEquals("name", actorClassMapper.attribute2property.get("NAME"));
      assertEquals("surname", actorClassMapper.attribute2property.get("SURNAME"));
      assertEquals("ID", actorClassMapper.property2attribute.get("id"));
      assertEquals("NAME", actorClassMapper.property2attribute.get("name"));
      assertEquals("SURNAME", actorClassMapper.property2attribute.get("surname"));

      OEntity filmActorEntity = mapper.getDataBaseSchema().getEntityByName("FILM_ACTOR");
      assertEquals(1, mapper.getClassMappersByVertex(film2actorVertexType).size());
      OClassMapper filmActorClassMapper =  mapper.getClassMappersByVertex(film2actorVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(filmActorEntity).size());
      assertEquals(filmActorClassMapper, mapper.getClassMappersByEntity(filmActorEntity).get(0));
      assertEquals(filmActorClassMapper.getEntity(), filmActorEntity);
      assertEquals(filmActorClassMapper.getVertexType(), film2actorVertexType);

      assertEquals(2, filmActorClassMapper.attribute2property.size());
      assertEquals(2, filmActorClassMapper.property2attribute.size());
      assertEquals("filmId", filmActorClassMapper.attribute2property.get("FILM_ID"));
      assertEquals("actorId", filmActorClassMapper.attribute2property.get("ACTOR_ID"));
      assertEquals("FILM_ID", filmActorClassMapper.property2attribute.get("filmId"));
      assertEquals("ACTOR_ID", filmActorClassMapper.property2attribute.get("actorId"));

      // Relationships-Edges Mapping

      Iterator<ORelationship> it = filmActorEntity.getOutRelationships().iterator();
      ORelationship hasActorRelationship = it.next();
      ORelationship hasFilmRelationship = it.next();
      assertFalse(it.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(filmEdgeType, mapper.getRelationship2edgeType().get(hasFilmRelationship));
      assertEquals(actorEdgeType, mapper.getRelationship2edgeType().get(hasActorRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(filmEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(filmEdgeType).contains(hasFilmRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(actorEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(actorEdgeType).contains(hasActorRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

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
   *  Two tables: 1 Foreign and 1 Parent (parent has an inner referential integrity).
   *  The primary key is imported both by the foreign table and the attribute of the parent table itself.
   */

  public void test6() {

    Connection connection = null;
    Statement st = null;

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

      this.mapper = new OER2GraphMapper(this.sourceDBInfo, null, null, null);
      mapper.buildSourceDatabaseSchema();
      mapper.buildGraphModel(new OJavaConventionNameResolver());


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfModelVertices);
      assertEquals(2, context.getStatistics().builtModelVertexTypes);
      assertEquals(2, context.getStatistics().totalNumberOfModelEdges);
      assertEquals(2, context.getStatistics().builtModelEdgeTypes);


      /*
       *  Testing built graph model
       */
      OVertexType employeeVertexType = mapper.getGraphModel().getVertexTypeByName("Employee");
      OVertexType projectVertexType = mapper.getGraphModel().getVertexTypeByName("Project");
      OEdgeType projectManagerEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasProjectManager");
      OEdgeType mgrEdgeType = mapper.getGraphModel().getEdgeTypeByName("HasMgr");


      // vertices check
      Assert.assertEquals(2, mapper.getGraphModel().getVerticesType().size());
      assertNotNull(employeeVertexType);
      assertNotNull(projectVertexType);

      // properties check
      assertEquals(3, employeeVertexType.getProperties().size());

      assertNotNull(employeeVertexType.getPropertyByName("empId"));
      assertEquals("empId", employeeVertexType.getPropertyByName("empId").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("empId").getOriginalType());
      assertEquals(1, employeeVertexType.getPropertyByName("empId").getOrdinalPosition());
      assertEquals(true, employeeVertexType.getPropertyByName("empId").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("mgrId"));
      assertEquals("mgrId", employeeVertexType.getPropertyByName("mgrId").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("mgrId").getOriginalType());
      assertEquals(2, employeeVertexType.getPropertyByName("mgrId").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("mgrId").isFromPrimaryKey());

      assertNotNull(employeeVertexType.getPropertyByName("name"));
      assertEquals("name", employeeVertexType.getPropertyByName("name").getName());
      assertEquals("VARCHAR", employeeVertexType.getPropertyByName("name").getOriginalType());
      assertEquals(3, employeeVertexType.getPropertyByName("name").getOrdinalPosition());
      assertEquals(false, employeeVertexType.getPropertyByName("name").isFromPrimaryKey());

      assertEquals(3, projectVertexType.getProperties().size());

      assertNotNull(projectVertexType.getPropertyByName("id"));
      assertEquals("id", projectVertexType.getPropertyByName("id").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("id").getOriginalType());
      assertEquals(1, projectVertexType.getPropertyByName("id").getOrdinalPosition());
      assertEquals(true, projectVertexType.getPropertyByName("id").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("title"));
      assertEquals("title", projectVertexType.getPropertyByName("title").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("title").getOriginalType());
      assertEquals(2, projectVertexType.getPropertyByName("title").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("title").isFromPrimaryKey());

      assertNotNull(projectVertexType.getPropertyByName("projectManager"));
      assertEquals("projectManager", projectVertexType.getPropertyByName("projectManager").getName());
      assertEquals("VARCHAR", projectVertexType.getPropertyByName("projectManager").getOriginalType());
      assertEquals(3, projectVertexType.getPropertyByName("projectManager").getOrdinalPosition());
      assertEquals(false, projectVertexType.getPropertyByName("projectManager").isFromPrimaryKey());

      // edges check
      Assert.assertEquals(2, mapper.getGraphModel().getEdgesType().size());
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

      /*
       * Rules check
       */

      // Classes Mapping

      assertEquals(2, mapper.getVertexType2classMappers().size());
      assertEquals(2, mapper.getEntity2classMappers().size());

      OEntity employeeEntity = mapper.getDataBaseSchema().getEntityByName("EMPLOYEE");
      assertEquals(1, mapper.getClassMappersByVertex(employeeVertexType).size());
      OClassMapper employeeClassMapper = mapper.getClassMappersByVertex(employeeVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(employeeEntity).size());
      assertEquals(employeeClassMapper, mapper.getClassMappersByEntity(employeeEntity).get(0));
      assertEquals(employeeClassMapper.getEntity(), employeeEntity);
      assertEquals(employeeClassMapper.getVertexType(), employeeVertexType);

      assertEquals(3, employeeClassMapper.attribute2property.size());
      assertEquals(3, employeeClassMapper.property2attribute.size());
      assertEquals("empId", employeeClassMapper.attribute2property.get("EMP_ID"));
      assertEquals("mgrId", employeeClassMapper.attribute2property.get("MGR_ID"));
      assertEquals("name", employeeClassMapper.attribute2property.get("NAME"));
      assertEquals("EMP_ID", employeeClassMapper.property2attribute.get("empId"));
      assertEquals("MGR_ID", employeeClassMapper.property2attribute.get("mgrId"));
      assertEquals("NAME", employeeClassMapper.property2attribute.get("name"));

      OEntity projectEntity = mapper.getDataBaseSchema().getEntityByName("PROJECT");
      assertEquals(1, mapper.getClassMappersByVertex(projectVertexType).size());
      OClassMapper projectClassMapper =  mapper.getClassMappersByVertex(projectVertexType).get(0);
      assertEquals(1, mapper.getClassMappersByEntity(projectEntity).size());
      assertEquals(projectClassMapper, mapper.getClassMappersByEntity(projectEntity).get(0));
      assertEquals(projectClassMapper.getEntity(), projectEntity);
      assertEquals(projectClassMapper.getVertexType(), projectVertexType);

      assertEquals(3, projectClassMapper.attribute2property.size());
      assertEquals(3, projectClassMapper.property2attribute.size());
      assertEquals("id", projectClassMapper.attribute2property.get("ID"));
      assertEquals("title", projectClassMapper.attribute2property.get("TITLE"));
      assertEquals("projectManager", projectClassMapper.attribute2property.get("PROJECT_MANAGER"));
      assertEquals("ID", projectClassMapper.property2attribute.get("id"));
      assertEquals("TITLE", projectClassMapper.property2attribute.get("title"));
      assertEquals("PROJECT_MANAGER", projectClassMapper.property2attribute.get("projectManager"));

      // Relationships-Edges Mapping

      Iterator<ORelationship> it = employeeEntity.getOutRelationships().iterator();
      ORelationship hasManagerRelationship = it.next();
      assertFalse(it.hasNext());
      it = projectEntity.getOutRelationships().iterator();
      ORelationship hasProjectManagerRelationship = it.next();
      assertFalse(it.hasNext());

      assertEquals(2, mapper.getRelationship2edgeType().size());
      assertEquals(mgrEdgeType, mapper.getRelationship2edgeType().get(hasManagerRelationship));
      assertEquals(projectManagerEdgeType, mapper.getRelationship2edgeType().get(hasProjectManagerRelationship));

      assertEquals(2, mapper.getEdgeType2relationships().size());
      assertEquals(1, mapper.getEdgeType2relationships().get(mgrEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(mgrEdgeType).contains(hasManagerRelationship));
      assertEquals(1, mapper.getEdgeType2relationships().get(projectManagerEdgeType).size());
      assertTrue(mapper.getEdgeType2relationships().get(projectManagerEdgeType).contains(hasProjectManagerRelationship));

      // JoinVertexes-AggregatorEdges Mapping

      assertEquals(0, mapper.getJoinVertex2aggregatorEdges().size());

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
