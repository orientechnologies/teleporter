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

package com.orientechnologies.teleporter.test.rdbms.mapper;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OCanonicalRelationship;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class SourceSchemaBuildingTest {

  private OER2GraphMapper    mapper;
  private OTeleporterContext context;
  private ODBQueryEngine     dbQueryEngine;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";
  private OSourceDatabaseInfo sourceDBInfo;
  private String outParentDirectory = "embedded:target/";


  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance(this.outParentDirectory);
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setMessageHandler(new OTeleporterMessageHandler(0));
    this.sourceDBInfo = new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
    this.mapper = new OER2GraphMapper(sourceDBInfo, null, null, null);
  }

  //@Test

  /*
   *  Two Foreign tables and one Parent with a simple primary key imported from the parent table.
   */

  public void test1() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table PARENT_AUTHOR (AUTHOR_ID varchar(256) not null,"
          + " AUTHOR_NAME varchar(256) not null, primary key (AUTHOR_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table FOREIGN_BOOK (BOOK_ID varchar(256) not null, TITLE  varchar(256),"
          + " AUTHOR varchar(256) not null, primary key (BOOK_ID), foreign key (AUTHOR) references PARENT_AUTHOR(AUTHOR_ID))";
      st.execute(foreignTableBuilding);

      this.mapper.buildSourceDatabaseSchema();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);
      assertEquals(1, context.getStatistics().totalNumberOfRelationships);
      assertEquals(1, context.getStatistics().builtRelationships);


      /*
       *  Testing built source db schema 
       */

      OEntity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_AUTHOR");
      OEntity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_BOOK");

      // entities check
      Assert.assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(parentEntity);
      assertNotNull(foreignEntity);

      // attributes check
      assertEquals(2, parentEntity.getAttributes().size());

      assertNotNull(parentEntity.getAttributeByName("AUTHOR_ID"));
      assertEquals("AUTHOR_ID", parentEntity.getAttributeByName("AUTHOR_ID").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("AUTHOR_ID").getDataType());
      assertEquals(1, parentEntity.getAttributeByName("AUTHOR_ID").getOrdinalPosition());
      assertEquals("PARENT_AUTHOR", parentEntity.getAttributeByName("AUTHOR_ID").getBelongingEntity().getName());

      assertNotNull(parentEntity.getAttributeByName("AUTHOR_NAME"));
      assertEquals("AUTHOR_NAME", parentEntity.getAttributeByName("AUTHOR_NAME").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("AUTHOR_NAME").getDataType());
      assertEquals(2, parentEntity.getAttributeByName("AUTHOR_NAME").getOrdinalPosition());
      assertEquals("PARENT_AUTHOR", parentEntity.getAttributeByName("AUTHOR_NAME").getBelongingEntity().getName());

      assertEquals(3, foreignEntity.getAttributes().size());

      assertNotNull(foreignEntity.getAttributeByName("BOOK_ID"));
      assertEquals("BOOK_ID", foreignEntity.getAttributeByName("BOOK_ID").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("BOOK_ID").getDataType());
      assertEquals(1, foreignEntity.getAttributeByName("BOOK_ID").getOrdinalPosition());
      assertEquals("FOREIGN_BOOK", foreignEntity.getAttributeByName("BOOK_ID").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", foreignEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(2, foreignEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("FOREIGN_BOOK", foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("AUTHOR"));
      assertEquals("AUTHOR", foreignEntity.getAttributeByName("AUTHOR").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("AUTHOR").getDataType());
      assertEquals(3, foreignEntity.getAttributeByName("AUTHOR").getOrdinalPosition());
      assertEquals("FOREIGN_BOOK", foreignEntity.getAttributeByName("AUTHOR").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(1, foreignEntity.getOutCanonicalRelationships().size());
      assertEquals(0, parentEntity.getOutCanonicalRelationships().size());
      assertEquals(0, foreignEntity.getInCanonicalRelationships().size());
      assertEquals(1, parentEntity.getInCanonicalRelationships().size());
      assertEquals(0, parentEntity.getForeignKeys().size());
      assertEquals(1, foreignEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("PARENT_AUTHOR", currentRelationship.getParentEntity().getName());
      assertEquals("FOREIGN_BOOK", currentRelationship.getForeignEntity().getName());
      assertEquals(parentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(foreignEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("AUTHOR", foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("AUTHOR_ID", parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

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

  //@Test

  /*
   *  Two Foreign tables and one Parent with a composite primary key imported from the parent table.
   */

  public void test2() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table PARENT_AUTHOR (AUTHOR_NAME varchar(256) not null,"
          + " AUTHOR_SURNAME varchar(256) not null, AGE INTEGER, primary key (AUTHOR_NAME,AUTHOR_SURNAME))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table FOREIGN_BOOK (TITLE  varchar(256),"
          + " AUTHOR_NAME varchar(256) not null, AUTHOR_SURNAME varchar(256) not null, primary key (TITLE),"
          + " foreign key (AUTHOR_NAME,AUTHOR_SURNAME) references PARENT_AUTHOR(AUTHOR_NAME,AUTHOR_SURNAME))";
      st.execute(foreignTableBuilding);

      mapper.buildSourceDatabaseSchema();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);
      assertEquals(1, context.getStatistics().totalNumberOfRelationships);
      assertEquals(1, context.getStatistics().builtRelationships);


      /*
       *  Testing built source db schema 
       */

      OEntity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_AUTHOR");
      OEntity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_BOOK");

      // entities check
      Assert.assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(1, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(parentEntity);
      assertNotNull(foreignEntity);

      // attributes check
      assertEquals(3, parentEntity.getAttributes().size());

      assertNotNull(parentEntity.getAttributeByName("AUTHOR_NAME"));
      assertEquals("AUTHOR_NAME", parentEntity.getAttributeByName("AUTHOR_NAME").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("AUTHOR_NAME").getDataType());
      assertEquals(1, parentEntity.getAttributeByName("AUTHOR_NAME").getOrdinalPosition());
      assertEquals("PARENT_AUTHOR", parentEntity.getAttributeByName("AUTHOR_NAME").getBelongingEntity().getName());

      assertNotNull(parentEntity.getAttributeByName("AUTHOR_SURNAME"));
      assertEquals("AUTHOR_SURNAME", parentEntity.getAttributeByName("AUTHOR_SURNAME").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("AUTHOR_SURNAME").getDataType());
      assertEquals(2, parentEntity.getAttributeByName("AUTHOR_SURNAME").getOrdinalPosition());
      assertEquals("PARENT_AUTHOR", parentEntity.getAttributeByName("AUTHOR_SURNAME").getBelongingEntity().getName());

      assertNotNull(parentEntity.getAttributeByName("AGE"));
      assertEquals("AGE", parentEntity.getAttributeByName("AGE").getName());
      assertEquals("INTEGER", parentEntity.getAttributeByName("AGE").getDataType());
      assertEquals(3, parentEntity.getAttributeByName("AGE").getOrdinalPosition());
      assertEquals("PARENT_AUTHOR", parentEntity.getAttributeByName("AGE").getBelongingEntity().getName());

      assertEquals(3, foreignEntity.getAttributes().size());

      assertNotNull(foreignEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", foreignEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(1, foreignEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("FOREIGN_BOOK", foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("AUTHOR_NAME"));
      assertEquals("AUTHOR_NAME", foreignEntity.getAttributeByName("AUTHOR_NAME").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("AUTHOR_NAME").getDataType());
      assertEquals(2, foreignEntity.getAttributeByName("AUTHOR_NAME").getOrdinalPosition());
      assertEquals("FOREIGN_BOOK", foreignEntity.getAttributeByName("AUTHOR_NAME").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("AUTHOR_SURNAME"));
      assertEquals("AUTHOR_SURNAME", foreignEntity.getAttributeByName("AUTHOR_SURNAME").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("AUTHOR_SURNAME").getDataType());
      assertEquals(3, foreignEntity.getAttributeByName("AUTHOR_SURNAME").getOrdinalPosition());
      assertEquals("FOREIGN_BOOK", foreignEntity.getAttributeByName("AUTHOR_SURNAME").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(1, foreignEntity.getOutCanonicalRelationships().size());
      assertEquals(0, parentEntity.getOutCanonicalRelationships().size());
      assertEquals(0, foreignEntity.getInCanonicalRelationships().size());
      assertEquals(1, parentEntity.getInCanonicalRelationships().size());
      assertEquals(0, parentEntity.getForeignKeys().size());
      assertEquals(1, foreignEntity.getForeignKeys().size());

      Iterator<OCanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("PARENT_AUTHOR", currentRelationship.getParentEntity().getName());
      assertEquals("FOREIGN_BOOK", currentRelationship.getForeignEntity().getName());
      assertEquals(parentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(foreignEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("AUTHOR_NAME", foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("AUTHOR_SURNAME", foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(1).getName());
      assertEquals("AUTHOR_NAME", parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("AUTHOR_SURNAME", parentEntity.getPrimaryKey().getInvolvedAttributes().get(1).getName());

      assertFalse(it.hasNext());

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

  //@Test

  /*
   *  Two Foreign tables and one Parent with a simple primary key imported twice from the parent table.
   */

  public void test3() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table PARENT_PERSON (PERSON_ID varchar(256) not null,"
          + " NAME varchar(256) not null, primary key (PERSON_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table FOREIGN_ARTICLE (TITLE  varchar(256),"
          + " AUTHOR varchar(256) not null, TRANSLATOR varchar(256) not null, primary key (TITLE),"
          + " foreign key (AUTHOR) references PARENT_PERSON(PERSON_ID),"
          + " foreign key (TRANSLATOR) references PARENT_PERSON(PERSON_ID))";
      st.execute(foreignTableBuilding);

      mapper.buildSourceDatabaseSchema();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);


      /*
       *  Testing built source db schema 
       */

      OEntity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_PERSON");
      OEntity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_ARTICLE");

      // entities check
      Assert.assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(parentEntity);
      assertNotNull(foreignEntity);

      // attributes check
      assertEquals(2, parentEntity.getAttributes().size());

      assertNotNull(parentEntity.getAttributeByName("PERSON_ID"));
      assertEquals("PERSON_ID", parentEntity.getAttributeByName("PERSON_ID").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("PERSON_ID").getDataType());
      assertEquals(1, parentEntity.getAttributeByName("PERSON_ID").getOrdinalPosition());
      assertEquals("PARENT_PERSON", parentEntity.getAttributeByName("PERSON_ID").getBelongingEntity().getName());

      assertNotNull(parentEntity.getAttributeByName("NAME"));
      assertEquals("NAME", parentEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("NAME").getDataType());
      assertEquals(2, parentEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals("PARENT_PERSON", parentEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertEquals(3, foreignEntity.getAttributes().size());

      assertNotNull(foreignEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", foreignEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(1, foreignEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("FOREIGN_ARTICLE", foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("AUTHOR"));
      assertEquals("AUTHOR", foreignEntity.getAttributeByName("AUTHOR").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("AUTHOR").getDataType());
      assertEquals(2, foreignEntity.getAttributeByName("AUTHOR").getOrdinalPosition());
      assertEquals("FOREIGN_ARTICLE", foreignEntity.getAttributeByName("AUTHOR").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("TRANSLATOR"));
      assertEquals("TRANSLATOR", foreignEntity.getAttributeByName("TRANSLATOR").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("TRANSLATOR").getDataType());
      assertEquals(3, foreignEntity.getAttributeByName("TRANSLATOR").getOrdinalPosition());
      assertEquals("FOREIGN_ARTICLE", foreignEntity.getAttributeByName("TRANSLATOR").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(2, foreignEntity.getOutCanonicalRelationships().size());
      assertEquals(0, parentEntity.getOutCanonicalRelationships().size());
      assertEquals(0, foreignEntity.getInCanonicalRelationships().size());
      assertEquals(2, parentEntity.getInCanonicalRelationships().size());
      assertEquals(0, parentEntity.getForeignKeys().size());
      assertEquals(2, foreignEntity.getForeignKeys().size());

      // first relationship
      Iterator<OCanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("PARENT_PERSON", currentRelationship.getParentEntity().getName());
      assertEquals("FOREIGN_ARTICLE", currentRelationship.getForeignEntity().getName());
      assertEquals(parentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(foreignEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("AUTHOR", foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("PERSON_ID", parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      // second relationship
      currentRelationship = it.next();
      assertEquals("PARENT_PERSON", currentRelationship.getParentEntity().getName());
      assertEquals("FOREIGN_ARTICLE", currentRelationship.getForeignEntity().getName());
      assertEquals(parentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(foreignEntity.getForeignKeys().get(1), currentRelationship.getForeignKey());

      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("TRANSLATOR", foreignEntity.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName());
      assertEquals("PERSON_ID", parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

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

  //@Test

  /*
   *  Two Foreign tables and one Parent with a composite primary key imported twice from the parent table.
   */

  public void test4() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table PARENT_PERSON (NAME varchar(256) not null,"
          + " SURNAME varchar(256) not null, primary key (NAME,SURNAME))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table FOREIGN_ARTICLE (TITLE  varchar(256),"
          + " AUTHOR_NAME varchar(256) not null, AUTHOR_SURNAME varchar(256) not null, TRANSLATOR_NAME varchar(256) not null,"
          + " TRANSLATOR_SURNAME varchar(256) not null, primary key (TITLE),"
          + " foreign key (AUTHOR_NAME,AUTHOR_SURNAME) references PARENT_PERSON(NAME,SURNAME),"
          + " foreign key (TRANSLATOR_NAME,TRANSLATOR_SURNAME) references PARENT_PERSON(NAME,SURNAME))";
      st.execute(foreignTableBuilding);

      mapper.buildSourceDatabaseSchema();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);


      /*
       *  Testing built source db schema 
       */

      OEntity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_PERSON");
      OEntity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_ARTICLE");

      // entities check
      Assert.assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(parentEntity);
      assertNotNull(foreignEntity);

      // attributes check
      assertEquals(2, parentEntity.getAttributes().size());

      assertNotNull(parentEntity.getAttributeByName("NAME"));
      assertEquals("NAME", parentEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("NAME").getDataType());
      assertEquals(1, parentEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals("PARENT_PERSON", parentEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertNotNull(parentEntity.getAttributeByName("SURNAME"));
      assertEquals("SURNAME", parentEntity.getAttributeByName("SURNAME").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("SURNAME").getDataType());
      assertEquals(2, parentEntity.getAttributeByName("SURNAME").getOrdinalPosition());
      assertEquals("PARENT_PERSON", parentEntity.getAttributeByName("SURNAME").getBelongingEntity().getName());

      assertEquals(5, foreignEntity.getAttributes().size());

      assertNotNull(foreignEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", foreignEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(1, foreignEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("FOREIGN_ARTICLE", foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("AUTHOR_NAME"));
      assertEquals("AUTHOR_NAME", foreignEntity.getAttributeByName("AUTHOR_NAME").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("AUTHOR_NAME").getDataType());
      assertEquals(2, foreignEntity.getAttributeByName("AUTHOR_NAME").getOrdinalPosition());
      assertEquals("FOREIGN_ARTICLE", foreignEntity.getAttributeByName("AUTHOR_NAME").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("AUTHOR_SURNAME"));
      assertEquals("AUTHOR_SURNAME", foreignEntity.getAttributeByName("AUTHOR_SURNAME").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("AUTHOR_SURNAME").getDataType());
      assertEquals(3, foreignEntity.getAttributeByName("AUTHOR_SURNAME").getOrdinalPosition());
      assertEquals("FOREIGN_ARTICLE", foreignEntity.getAttributeByName("AUTHOR_SURNAME").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("TRANSLATOR_NAME"));
      assertEquals("TRANSLATOR_NAME", foreignEntity.getAttributeByName("TRANSLATOR_NAME").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("TRANSLATOR_NAME").getDataType());
      assertEquals(4, foreignEntity.getAttributeByName("TRANSLATOR_NAME").getOrdinalPosition());
      assertEquals("FOREIGN_ARTICLE", foreignEntity.getAttributeByName("TRANSLATOR_NAME").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("TRANSLATOR_SURNAME"));
      assertEquals("TRANSLATOR_SURNAME", foreignEntity.getAttributeByName("TRANSLATOR_SURNAME").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("TRANSLATOR_SURNAME").getDataType());
      assertEquals(5, foreignEntity.getAttributeByName("TRANSLATOR_SURNAME").getOrdinalPosition());
      assertEquals("FOREIGN_ARTICLE", foreignEntity.getAttributeByName("TRANSLATOR_SURNAME").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(2, foreignEntity.getOutCanonicalRelationships().size());
      assertEquals(0, parentEntity.getOutCanonicalRelationships().size());
      assertEquals(0, foreignEntity.getInCanonicalRelationships().size());
      assertEquals(2, parentEntity.getInCanonicalRelationships().size());
      assertEquals(0, parentEntity.getForeignKeys().size());
      assertEquals(2, foreignEntity.getForeignKeys().size());

      // first relationship
      Iterator<OCanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("PARENT_PERSON", currentRelationship.getParentEntity().getName());
      assertEquals("FOREIGN_ARTICLE", currentRelationship.getForeignEntity().getName());
      assertEquals(parentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(foreignEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("AUTHOR_NAME", foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("AUTHOR_SURNAME", foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(1).getName());
      assertEquals("NAME", parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("SURNAME", parentEntity.getPrimaryKey().getInvolvedAttributes().get(1).getName());

      // second relationship
      currentRelationship = it.next();
      assertEquals("PARENT_PERSON", currentRelationship.getParentEntity().getName());
      assertEquals("FOREIGN_ARTICLE", currentRelationship.getForeignEntity().getName());
      assertEquals(parentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(foreignEntity.getForeignKeys().get(1), currentRelationship.getForeignKey());
      assertFalse(it.hasNext());

      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("TRANSLATOR_NAME", foreignEntity.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName());
      assertEquals("TRANSLATOR_SURNAME", foreignEntity.getForeignKeys().get(1).getInvolvedAttributes().get(1).getName());
      assertEquals("NAME", parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());
      assertEquals("SURNAME", parentEntity.getPrimaryKey().getInvolvedAttributes().get(1).getName());

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

  //@Test

  /*
   *  Two tables: 1 Foreign and 1 Parent (parent has an inner referential integrity).
   *  The primary key is imported both by the foreign table and from the first attribute of the parent table itself.
   */

  public void test5() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String parentTableBuilding = "create memory table PARENT_EMPLOYEE (EMP_ID varchar(256) not null,"
          + " MGR_ID varchar(256) not null, NAME varchar(256) not null, primary key (EMP_ID), "
          + " foreign key (MGR_ID) references PARENT_EMPLOYEE(EMP_ID))";
      st = connection.createStatement();
      st.execute(parentTableBuilding);

      String foreignTableBuilding = "create memory table FOREIGN_PROJECT (PROJECT_ID  varchar(256),"
          + " TITLE varchar(256) not null, PROJECT_MANAGER varchar(256) not null, primary key (PROJECT_ID),"
          + " foreign key (PROJECT_MANAGER) references PARENT_EMPLOYEE(EMP_ID))";
      st.execute(foreignTableBuilding);

      mapper.buildSourceDatabaseSchema();


      /*
       *  Testing context information
       */

      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);


      /*
       *  Testing built source db schema 
       */

      OEntity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_EMPLOYEE");
      OEntity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_PROJECT");

      // entities check
      Assert.assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(parentEntity);
      assertNotNull(foreignEntity);

      // attributes check
      assertEquals(3, parentEntity.getAttributes().size());

      assertNotNull(parentEntity.getAttributeByName("EMP_ID"));
      assertEquals("EMP_ID", parentEntity.getAttributeByName("EMP_ID").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("EMP_ID").getDataType());
      assertEquals(1, parentEntity.getAttributeByName("EMP_ID").getOrdinalPosition());
      assertEquals("PARENT_EMPLOYEE", parentEntity.getAttributeByName("EMP_ID").getBelongingEntity().getName());

      assertNotNull(parentEntity.getAttributeByName("MGR_ID"));
      assertEquals("MGR_ID", parentEntity.getAttributeByName("MGR_ID").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("MGR_ID").getDataType());
      assertEquals(2, parentEntity.getAttributeByName("MGR_ID").getOrdinalPosition());
      assertEquals("PARENT_EMPLOYEE", parentEntity.getAttributeByName("MGR_ID").getBelongingEntity().getName());

      assertNotNull(parentEntity.getAttributeByName("NAME"));
      assertEquals("NAME", parentEntity.getAttributeByName("NAME").getName());
      assertEquals("VARCHAR", parentEntity.getAttributeByName("NAME").getDataType());
      assertEquals(3, parentEntity.getAttributeByName("NAME").getOrdinalPosition());
      assertEquals("PARENT_EMPLOYEE", parentEntity.getAttributeByName("NAME").getBelongingEntity().getName());

      assertEquals(3, foreignEntity.getAttributes().size());

      assertNotNull(foreignEntity.getAttributeByName("PROJECT_ID"));
      assertEquals("PROJECT_ID", foreignEntity.getAttributeByName("PROJECT_ID").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("PROJECT_ID").getDataType());
      assertEquals(1, foreignEntity.getAttributeByName("PROJECT_ID").getOrdinalPosition());
      assertEquals("FOREIGN_PROJECT", foreignEntity.getAttributeByName("PROJECT_ID").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("TITLE"));
      assertEquals("TITLE", foreignEntity.getAttributeByName("TITLE").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("TITLE").getDataType());
      assertEquals(2, foreignEntity.getAttributeByName("TITLE").getOrdinalPosition());
      assertEquals("FOREIGN_PROJECT", foreignEntity.getAttributeByName("TITLE").getBelongingEntity().getName());

      assertNotNull(foreignEntity.getAttributeByName("PROJECT_MANAGER"));
      assertEquals("PROJECT_MANAGER", foreignEntity.getAttributeByName("PROJECT_MANAGER").getName());
      assertEquals("VARCHAR", foreignEntity.getAttributeByName("PROJECT_MANAGER").getDataType());
      assertEquals(3, foreignEntity.getAttributeByName("PROJECT_MANAGER").getOrdinalPosition());
      assertEquals("FOREIGN_PROJECT", foreignEntity.getAttributeByName("PROJECT_MANAGER").getBelongingEntity().getName());

      // relationship, primary and foreign key check
      assertEquals(1, foreignEntity.getOutCanonicalRelationships().size());
      assertEquals(1, parentEntity.getOutCanonicalRelationships().size());
      assertEquals(0, foreignEntity.getInCanonicalRelationships().size());
      assertEquals(2, parentEntity.getInCanonicalRelationships().size());
      assertEquals(1, parentEntity.getForeignKeys().size());
      assertEquals(1, foreignEntity.getForeignKeys().size());

      // first relationship
      Iterator<OCanonicalRelationship> it = foreignEntity.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("PARENT_EMPLOYEE", currentRelationship.getParentEntity().getName());
      assertEquals("FOREIGN_PROJECT", currentRelationship.getForeignEntity().getName());
      assertEquals(parentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(foreignEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = parentEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("PROJECT_MANAGER", foreignEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("EMP_ID", parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      // second relationship
      it = parentEntity.getOutCanonicalRelationships().iterator();
      currentRelationship = it.next();
      assertEquals("PARENT_EMPLOYEE", currentRelationship.getParentEntity().getName());
      assertEquals("PARENT_EMPLOYEE", currentRelationship.getForeignEntity().getName());
      assertEquals(parentEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(parentEntity.getForeignKeys().get(0), currentRelationship.getForeignKey());

      currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("MGR_ID", parentEntity.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("EMP_ID", parentEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

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

  //@Test

  /*
   * Join table and 2 parent tables.
   */

  public void test6() {

    Connection connection = null;
    Statement st = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null, TITLE varchar(256) not null,"
          + " YEAR varchar(256) not null, DIRECTOR varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      String actorTableBuilding = "create memory table ACTOR (ID varchar(256) not null, NAME varchar(256) not null,"
          + " SURNAME varchar(256) not null, primary key (ID))";
      st = connection.createStatement();
      st.execute(actorTableBuilding);

      String joinTableBuilding = "create memory table FILM2ACTOR (FILM_ID  varchar(256) not null,"
          + " ACTOR_ID varchar(256) not null, SALARY varchar(256)," + " primary key (FILM_ID,ACTOR_ID),"
          + " foreign key (FILM_ID) references FILM(ID)," + " foreign key (ACTOR_ID) references ACTOR(ID))";
      st.execute(joinTableBuilding);

      connection.commit();

      mapper.buildSourceDatabaseSchema();


      /*
       *  Testing context information
       */

      assertEquals(3, context.getStatistics().totalNumberOfEntities);
      assertEquals(3, context.getStatistics().builtEntities);
      assertEquals(2, context.getStatistics().totalNumberOfRelationships);
      assertEquals(2, context.getStatistics().builtRelationships);


      /*
       *  Testing built source db schema 
       */

      OEntity filmEntity = mapper.getDataBaseSchema().getEntityByName("FILM");
      OEntity actorEntity = mapper.getDataBaseSchema().getEntityByName("ACTOR");
      OEntity film2actor = mapper.getDataBaseSchema().getEntityByName("FILM2ACTOR");

      // entities check
      Assert.assertEquals(3, mapper.getDataBaseSchema().getEntities().size());
      Assert.assertEquals(2, mapper.getDataBaseSchema().getCanonicalRelationships().size());
      assertNotNull(filmEntity);
      assertNotNull(actorEntity);
      assertNotNull(film2actor);

      // attributes check
      assertEquals(4, filmEntity.getAttributes().size());
      assertEquals(3, actorEntity.getAttributes().size());
      assertEquals(3, film2actor.getAttributes().size());

      // relationship, primary and foreign key check
      assertEquals(0, filmEntity.getOutCanonicalRelationships().size());
      assertEquals(0, actorEntity.getOutCanonicalRelationships().size());
      assertEquals(2, film2actor.getOutCanonicalRelationships().size());
      assertEquals(1, filmEntity.getInCanonicalRelationships().size());
      assertEquals(1, actorEntity.getInCanonicalRelationships().size());
      assertEquals(0, film2actor.getInCanonicalRelationships().size());
      assertEquals(0, filmEntity.getForeignKeys().size());
      assertEquals(0, actorEntity.getForeignKeys().size());
      assertEquals(2, film2actor.getForeignKeys().size());

      // first relationship
      Iterator<OCanonicalRelationship> it = film2actor.getOutCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship = it.next();
      assertEquals("ACTOR", currentRelationship.getParentEntity().getName());
      assertEquals("FILM2ACTOR", currentRelationship.getForeignEntity().getName());
      assertEquals(actorEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(film2actor.getForeignKeys().get(0), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it2 = actorEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship2 = it2.next();
      assertEquals(currentRelationship, currentRelationship2);

      assertEquals("ACTOR_ID", film2actor.getForeignKeys().get(0).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", actorEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      // second relationship
      currentRelationship = it.next();
      assertEquals("FILM", currentRelationship.getParentEntity().getName());
      assertEquals("FILM2ACTOR", currentRelationship.getForeignEntity().getName());
      assertEquals(filmEntity.getPrimaryKey(), currentRelationship.getPrimaryKey());
      assertEquals(film2actor.getForeignKeys().get(1), currentRelationship.getForeignKey());

      Iterator<OCanonicalRelationship> it3 = filmEntity.getInCanonicalRelationships().iterator();
      OCanonicalRelationship currentRelationship3 = it3.next();
      assertEquals(currentRelationship, currentRelationship3);

      assertEquals("FILM_ID", film2actor.getForeignKeys().get(1).getInvolvedAttributes().get(0).getName());
      assertEquals("ID", filmEntity.getPrimaryKey().getInvolvedAttributes().get(0).getName());

      assertFalse(it.hasNext());

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
