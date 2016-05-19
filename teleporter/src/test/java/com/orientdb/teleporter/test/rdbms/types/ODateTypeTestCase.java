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

package com.orientdb.teleporter.test.rdbms.types;

import com.orientdb.teleporter.context.OOutputStreamManager;
import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientdb.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientdb.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
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
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODateTypeTestCase {


  private OTeleporterContext context;
  private ODBMSNaiveStrategy importStrategy;
  private String outOrientGraphUri;

  @Before
  public void init() {
    this.context = new OTeleporterContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
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

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, YEAR interval year(4), primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,YEAR) values ("
          + "('F001','Pulp Fiction','1994'),"
          + "('F002','Shutter Island','2010'),"
          + "('F003','The Departed','2006'))";
      st.execute(filmFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null, context);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("STRING", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("year").getType().toString());

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
      orientGraph.drop();
      orientGraph.shutdown();
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

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, YEAR date, primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,YEAR) values ("
          + "('F001','Pulp Fiction','1994-09-10'),"
          + "('F002','Shutter Island','2010-02-13'),"
          + "('F003','The Departed','2006-09-26'))";
      st.execute(filmFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null, context);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("DATE", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("year").getType().toString());


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
    orientGraph.drop();
    orientGraph.shutdown();
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

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, YEAR date not null, LAST_UPDATE timestamp , primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,YEAR,LAST_UPDATE) values ("
          + "('F001','Pulp Fiction','1994-09-10','2012-08-08 20:08:08'),"
          + "('F002','Shutter Island','2010-02-13','2012-08-08 20:08:08'),"
          + "('F003','The Departed','2006-09-26','2012-08-08 20:08:08'))";
      st.execute(filmFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null, context);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("DATETIME", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());


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
    orientGraph.drop();
    orientGraph.shutdown();
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

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, YEAR date not null, LAST_UPDATE timestamp with time zone, primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,YEAR,LAST_UPDATE) values ("
          + "('F001','Pulp Fiction','1994-09-10','2012-08-08 20:08:08+8:00'),"
          + "('F002','Shutter Island','2010-02-13','2012-08-08 20:08:08+8:00'),"
          + "('F003','The Departed','2006-09-26','2012-08-08 20:08:08+8:00'))";
      st.execute(filmFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null, context);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("DATETIME", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());


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
    orientGraph.drop();
    orientGraph.shutdown();
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

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, YEAR date not null, LAST_UPDATE time , primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,YEAR,LAST_UPDATE) values ("
          + "('F001','Pulp Fiction','1994-09-10','20:08:08.034900'),"
          + "('F002','Shutter Island','2010-02-13','20:08:08.034900'),"
          + "('F003','The Departed','2006-09-26','20:08:08.034900'))";
      st.execute(filmFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null, context);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("STRING", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());


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
    orientGraph.drop();
    orientGraph.shutdown();
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

      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      // Tables Building

      String filmTableBuilding = "create memory table FILM (ID varchar(256) not null,"+
          " TITLE varchar(256) not null, YEAR date not null, LAST_UPDATE time , primary key (ID))";
      st = connection.createStatement();
      st.execute(filmTableBuilding);


      // Records Inserting

      String filmFilling = "insert into FILM (ID,TITLE,YEAR,LAST_UPDATE) values ("
          + "('F001','Pulp Fiction','1994-09-10','20:08:08.034900-8:00'),"
          + "('F002','Shutter Island','2010-02-13','20:08:08.034900-8:00'),"
          + "('F003','The Departed','2006-09-26','20:08:08.034900-8:00'))";
      st.execute(filmFilling);

      this.importStrategy.executeStrategy("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, "basicDBMapper", null, "java", null, null, null, context);


      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      assertEquals("STRING", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());


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
    orientGraph.drop();
    orientGraph.shutdown();
  }

}


