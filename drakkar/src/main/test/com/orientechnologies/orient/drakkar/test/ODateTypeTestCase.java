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

package com.orientechnologies.orient.drakkar.test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.OOutputStreamManager;
import com.orientechnologies.orient.drakkar.importengine.ODB2GraphImportEngine;
import com.orientechnologies.orient.drakkar.mapper.OER2GraphMapper;
import com.orientechnologies.orient.drakkar.mapper.OSource2GraphMapper;
import com.orientechnologies.orient.drakkar.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.orient.drakkar.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.orient.drakkar.writer.OGraphModelWriter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ODateTypeTestCase {


  private OSource2GraphMapper mapper;
  private ODrakkarContext context;
  private OGraphModelWriter modelWriter;
  private ODB2GraphImportEngine importEngine;
  private String outOrientGraphUri;

  @Before
  public void init() {
    this.context = new ODrakkarContext();
    this.context.setOutputManager(new OOutputStreamManager(0));
    this.context.setNameResolver(new OJavaConventionNameResolver());
    this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
    this.modelWriter = new OGraphModelWriter();
    this.importEngine = new ODB2GraphImportEngine();
    this.outOrientGraphUri = "memory:testOrientDB";
  }

/*
 * Custom year type test.
 * Conversion to OType.STRING.
 */
  @Test
  public void test() {

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

      String filmFilling = "INSERT INTO FILM (ID,TITLE,YEAR) VALUES ("
          + "('F001','Pulp Fiction','1994'),"
          + "('F002','Shutter Island','2010'),"
          + "('F003','The Departed','2006'))";
      st.execute(filmFilling);
      
      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "");
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(((OER2GraphMapper)mapper).getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);
      this.importEngine.executeImport("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, this.mapper, context);
      
     
      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check
      for(Vertex v: orientGraph.getVertices()) {
        System.out.print(v.getProperty("year"));
        System.out.println("\t" + orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("year").getType().toString());
      }
      
      assertEquals("STRING", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("year").getType().toString());
      
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

      String filmFilling = "INSERT INTO FILM (ID,TITLE,YEAR) VALUES ("
          + "('F001','Pulp Fiction','1994-09-10'),"
          + "('F002','Shutter Island','2010-02-13'),"
          + "('F003','The Departed','2006-09-26'))";
      st.execute(filmFilling);
      
      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "");
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(((OER2GraphMapper)mapper).getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);
      this.importEngine.executeImport("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, this.mapper, context);
      
     
      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check
      for(Vertex v: orientGraph.getVertices()) {
        System.out.print(v.getProperty("year"));
        System.out.println("\t" + orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("year").getType().toString());
      }
      
      assertEquals("DATETIME", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("year").getType().toString());

      
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

      String filmFilling = "INSERT INTO FILM (ID,TITLE,YEAR,LAST_UPDATE) VALUES ("
          + "('F001','Pulp Fiction','1994-09-10','2012-08-08 20:08:08'),"
          + "('F002','Shutter Island','2010-02-13','2012-08-08 20:08:08'),"
          + "('F003','The Departed','2006-09-26','2012-08-08 20:08:08'))";
      st.execute(filmFilling);
      
      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "");
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(((OER2GraphMapper)mapper).getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);
      this.importEngine.executeImport("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, this.mapper, context);
      
     
      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check
      for(Vertex v: orientGraph.getVertices()) {
        System.out.print(v.getProperty("lastUpdate"));
        System.out.println("\t" + orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());
      }
      
      assertEquals("DATETIME", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());

      
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

      String filmFilling = "INSERT INTO FILM (ID,TITLE,YEAR,LAST_UPDATE) VALUES ("
          + "('F001','Pulp Fiction','1994-09-10','2012-08-08 20:08:08+8:00'),"
          + "('F002','Shutter Island','2010-02-13','2012-08-08 20:08:08+8:00'),"
          + "('F003','The Departed','2006-09-26','2012-08-08 20:08:08+8:00'))";
      st.execute(filmFilling);
      
      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "");
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(((OER2GraphMapper)mapper).getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);
      this.importEngine.executeImport("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, this.mapper, context);
      
     
      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check
      for(Vertex v: orientGraph.getVertices()) {
        System.out.print(v.getProperty("lastUpdate"));
        System.out.println("\t" + orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());
      }
      
      assertEquals("DATETIME", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());

      
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

      String filmFilling = "INSERT INTO FILM (ID,TITLE,YEAR,LAST_UPDATE) VALUES ("
          + "('F001','Pulp Fiction','1994-09-10','20:08:08.034900'),"
          + "('F002','Shutter Island','2010-02-13','20:08:08.034900'),"
          + "('F003','The Departed','2006-09-26','20:08:08.034900'))";
      st.execute(filmFilling);
      
      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "");
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(((OER2GraphMapper)mapper).getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);
      this.importEngine.executeImport("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, this.mapper, context);
      
     
      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check
      for(Vertex v: orientGraph.getVertices()) {
        System.out.print(v.getProperty("lastUpdate"));
        System.out.println("\t" + orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());
      }
      
      assertEquals("STRING", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());

      
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

      String filmFilling = "INSERT INTO FILM (ID,TITLE,YEAR,LAST_UPDATE) VALUES ("
          + "('F001','Pulp Fiction','1994-09-10','20:08:08.034900-8:00'),"
          + "('F002','Shutter Island','2010-02-13','20:08:08.034900-8:00'),"
          + "('F003','The Departed','2006-09-26','20:08:08.034900-8:00'))";
      st.execute(filmFilling);
      
      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "");
      mapper.buildSourceSchema(this.context);
      mapper.buildGraphModel(new OJavaConventionNameResolver(), context);
      modelWriter.writeModelOnOrient(((OER2GraphMapper)mapper).getGraphModel(), new OHSQLDBDataTypeHandler(), this.outOrientGraphUri, context);
      this.importEngine.executeImport("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "", this.outOrientGraphUri, this.mapper, context);
      
     
      /*
       *  Testing built OrientDB
       */
      orientGraph = new OrientGraphNoTx(this.outOrientGraphUri);

      // vertices check
      for(Vertex v: orientGraph.getVertices()) {
        System.out.print(v.getProperty("lastUpdate"));
        System.out.println("\t" + orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());
      }
      
      assertEquals("STRING", orientGraph.getRawGraph().getMetadata().getSchema().getClass("Film").getProperty("lastUpdate").getType().toString());

      
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
    orientGraph.drop();
    orientGraph.shutdown();
  }
  
  
  
}


