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

package com.orientechnologies.teleporter.test.rdbms.main;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.teleporter.util.OFileManager;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public abstract class TeleporterInvocationTest {

  // arguments
  protected Map<String, String> arguments = new HashMap<String, String>();
  protected String[]   args;
  protected Connection dbConnection;
  private String driver   = "org.hsqldb.jdbc.JDBCDriver";
  private String jurl     = "jdbc:hsqldb:mem:mydb";
  private String username = "SA";
  private String password = "";

  // server configuration path
  private final String configurationPath = "orientdb-server-config.xml";
  private final String serverHome        = "target/server";

  protected void buildEnvironmentForExecution() {
    this.buildHSQLDBDatabaseToImport();
    try {
      OServerMain.create();
      //this.startServer(OServerMain.server());
    } catch (Exception e) {
      fail("Server instance not created correctly.");
    }
  }

  private void startServer(OServer server) throws Exception {
    System.out.println("Starting server from " + this.serverHome + "...");

    System.setProperty("ORIENTDB_HOME", this.serverHome);
    server.setServerRootDirectory(this.serverHome);
    server.startup(getClass().getClassLoader().getResourceAsStream(this.configurationPath));
    server.activate();
  }

  private String getDatabasePath(String databaseName) {
    return this.serverHome + "/databases/" + databaseName;
  }

  private void buildHSQLDBDatabaseToImport() {
    try {
      Class.forName(this.driver);
      this.dbConnection = DriverManager.getConnection(this.jurl, this.username, this.password);

      // Tables Building

      String directorTableBuilding = "create memory table DIRECTOR (ID varchar(256) not null, NAME  varchar(256),"
          + " SURNAME varchar(256) not null, primary key (ID))";
      Statement st = this.dbConnection.createStatement();
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
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public void prepareArguments() {
    this.arguments.put("-jdriver", "hypersql");
    this.arguments.put("-jurl", this.jurl);
    this.arguments.put("-ourl", "plocal:target/server/databases/testOrientDB");
    this.arguments.put("-juser", this.username);
    this.arguments.put("-jpasswd", this.password);
  }

  public void prepareArrayArgs() {

    args = new String[arguments.size() * 2];

    int i = 0;
    for (String key : arguments.keySet()) {
      args[i] = key;
      args[i + 1] = arguments.get(key);
      i = i + 2;
    }

  }

  protected void shutdownEnvironment() {

    this.closeAndDropSourceDatabase();
    this.closeAndDropOrientdbDatabase();
    //this.shutdownServer(OServerMain.server());
    this.purgeOrientdbServer();

    // REGISTER THE BINARY RECORD SERIALIZER TO SUPPORT ANY OF THE EXTERNAL FIELDS
    ORecordSerializerFactory.instance().register("ORecordSerializerBinary", new ORecordSerializerBinary());
  }

  public void shutdownServer(OServer server) {
    if (server != null) {
      server.shutdown();
    }
    closeStorages();
  }

  public void closeStorages() {
    for (OStorage s : Orient.instance().getStorages()) {
      if (s instanceof OLocalPaginatedStorage && ((OLocalPaginatedStorage) s).getStoragePath().startsWith(getDatabasePath(""))) {
        s.close(true, false);
//        Orient.instance().unregisterStorage(s);
      }
    }
  }

  private void closeAndDropSourceDatabase() {

    try {

      // Dropping Source DB Schema and OrientGraph
      String dbDropping = "drop schema public cascade";
      Statement st = this.dbConnection.createStatement();
      st.execute(dbDropping);
      this.dbConnection.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

  }

  private void closeAndDropOrientdbDatabase() {

    OrientGraphNoTx orientGraph = new OrientGraphNoTx("plocal:src/test/target/server/databases/testOrientDB");

    try {

      if (orientGraph != null) {
        orientGraph.drop();
        orientGraph.shutdown();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

  }

  private void purgeOrientdbServer() {

    try {
      OFileManager.deleteResource(this.serverHome);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

}
