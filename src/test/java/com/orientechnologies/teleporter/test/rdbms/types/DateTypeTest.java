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

package com.orientechnologies.teleporter.test.rdbms.types;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class DateTypeTest {

  private OTeleporterContext context;
  private ODBMSNaiveStrategy importStrategy;
  private ODBQueryEngine     dbQueryEngine;
  private String driver            = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl              = "jdbc:hsqldb:mem:mydb";
  private String username          = "SA";
  private String password          = "";
  private String outOrientGraphUri = "memory:testOrientDB";
  private OSourceDatabaseInfo sourceDBInfo;

  @Before
  public void init() {
    this.context = OTeleporterContext.newInstance();
    this.dbQueryEngine = new ODBQueryEngine(this.driver);
    this.sourceDBInfo = new OSourceDatabaseInfo("source", this.driver, this.jurl, this.username, this.password);
    this.context.setDbQueryEngine(this.dbQueryEngine);
    this.context.setMessageHandler(new OTeleporterMessageHandler(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.importStrategy = new ODBMSNaiveStrategy();
    this.outOrientGraphUri = "memory:testOrientDB";
  }

  /*
   * Custom year type test.
   * Conversion to OType.STRING.
   */

  @Test
  public void test1() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection(this.jurl, this.username, this.password);

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"
          + " TITLE varchar(256) not null, YEAR interval year(4), primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      // Records Inserting

      String filmFilling =
          "insert into FILM (ID,TITLE,YEAR) values (" + "('F001','Pulp Fiction','1994')," + "('F002','Shutter Island','2010'),"
              + "('F003','The Departed','2006'))";
      st.execute(filmFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("STRING",
          orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("year").getType().toString());

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
      if (orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }
    }
  }

  /*
   * Date type test.
   * Conversion to OType.DATETIME.
   */
  @Test
  public void test2() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", this.username, this.password);

      // Tables Building

      String filmTableBuilding =
          "create memory table FILM (ID varchar(256) not null," + " TITLE varchar(256) not null, YEAR date, primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,YEAR) values (" + "('F001','Pulp Fiction','1994-09-10'),"
          + "('F002','Shutter Island','2010-02-13')," + "('F003','The Departed','2006-09-26'))";
      st.execute(filmFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("DATE",
          orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("year").getType().toString());

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
    if (orientGraph != null) {
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }

  /*
   * Timestamp test.
   * Conversion to OType.DATETIME.
   */
  @Test
  public void test3() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", this.username, this.password);

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"
          + " TITLE varchar(256) not null, YEAR date not null, LAST_UPDATE timestamp , primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      // Records Inserting

      String filmFilling =
          "insert into FILM (ID,TITLE,YEAR,LAST_UPDATE) values (" + "('F001','Pulp Fiction','1994-09-10','2012-08-08 20:08:08'),"
              + "('F002','Shutter Island','2010-02-13','2012-08-08 20:08:08'),"
              + "('F003','The Departed','2006-09-26','2012-08-08 20:08:08'))";
      st.execute(filmFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("DATETIME",
          orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());

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
    if (orientGraph != null) {
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }

  /*
   * Timestamp with time zone test.
   * Conversion to OType.DATETIME.
   */
  @Test
  public void test4() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", this.username, this.password);

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"
          + " TITLE varchar(256) not null, YEAR date not null, LAST_UPDATE timestamp with time zone, primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,YEAR,LAST_UPDATE) values ("
          + "('F001','Pulp Fiction','1994-09-10','2012-08-08 20:08:08+8:00'),"
          + "('F002','Shutter Island','2010-02-13','2012-08-08 20:08:08+8:00'),"
          + "('F003','The Departed','2006-09-26','2012-08-08 20:08:08+8:00'))";
      st.execute(filmFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("DATETIME",
          orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());

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
    if (orientGraph != null) {
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }

  /*
   * Time test.
   * Conversion to OType.STRING.
   */
  @Test
  public void test5() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", this.username, this.password);

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"
          + " TITLE varchar(256) not null, YEAR date not null, LAST_UPDATE time , primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      // Records Inserting

      String filmFilling =
          "insert into FILM (ID,TITLE,YEAR,LAST_UPDATE) values (" + "('F001','Pulp Fiction','1994-09-10','20:08:08.034900'),"
              + "('F002','Shutter Island','2010-02-13','20:08:08.034900'),"
              + "('F003','The Departed','2006-09-26','20:08:08.034900'))";
      st.execute(filmFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("STRING",
          orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());

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
    if (orientGraph != null) {
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }

  /*
   * Time with time zone test.
   * Conversion to OType.STRING.
   */
  @Test
  public void test6() {

    Connection connection = null;
    Statement st = null;
    OrientGraphNoTx orientGraph = null;

    try {

      Class.forName(this.driver);
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", this.username, this.password);

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"
          + " TITLE varchar(256) not null, YEAR date not null, LAST_UPDATE time , primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);

      // Records Inserting

      String filmFilling =
          "insert into FILM (ID,TITLE,YEAR,LAST_UPDATE) values (" + "('F001','Pulp Fiction','1994-09-10','20:08:08.034900-8:00'),"
              + "('F002','Shutter Island','2010-02-13','20:08:08.034900-8:00'),"
              + "('F003','The Departed','2006-09-26','20:08:08.034900-8:00'))";
      st.execute(filmFilling);

      this.importStrategy
          .executeStrategy(this.sourceDBInfo, this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("STRING",
          orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());

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
    if (orientGraph != null) {
      orientGraph.drop();
      orientGraph.shutdown();
    }
  }

}


