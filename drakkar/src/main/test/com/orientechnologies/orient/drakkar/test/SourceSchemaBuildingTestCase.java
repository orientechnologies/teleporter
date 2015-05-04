package com.orientechnologies.orient.drakkar.test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.OOutputStreamManager;
import com.orientechnologies.orient.drakkar.mapper.OER2GraphMapper;
import com.orientechnologies.orient.drakkar.model.dbschema.OEntity;

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

public class SourceSchemaBuildingTestCase {

  private OER2GraphMapper mapper;
  private ODrakkarContext context;

  @Before
  public void init() {
    this.context = new ODrakkarContext();
    context.setOutputManager(new OOutputStreamManager(0));
  }


  @Test

  /*
   *  Two tables Foreign and Parent with a simple primary key imported from the parent table.
   */

  public void test1() {
    
    Connection connection = null;
    
    try {
      
      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String parentTableBuilding = "create memory table PARENT_AUTHOR (AUTHOR_ID varchar(256) not null,"+
          " AUTHOR_NAME varchar(256) not null, primary key (AUTHOR_ID))";
      Statement st = connection.createStatement();
      st.execute(parentTableBuilding);


      String foreignTableBuilding = "create memory table FOREIGN_BOOK (BOOK_ID varchar(256) not null, TITLE  varchar(256),"+
          " AUTHOR varchar(256) not null, primary key (BOOK_ID), foreign key (AUTHOR) references PARENT_AUTHOR(AUTHOR_ID))";
      st.execute(foreignTableBuilding);

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "");
      mapper.buildSourceSchema(this.context);

      
      /*
       *  Testing context information
       */
      
      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);      
      assertEquals(1, context.getStatistics().detectedRelationships);

      
      /*
       *  Testing built source db schema 
       */
      
      OEntity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_AUTHOR");
      OEntity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_BOOK");
      
      // entities check
      assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(1, mapper.getDataBaseSchema().getRelationships().size());
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
      assertEquals(1, foreignEntity.getRelationships().size());
      assertEquals(0, parentEntity.getRelationships().size());
      
      assertEquals("PARENT_AUTHOR", foreignEntity.getRelationships().get(0).getParentEntityName());
      assertEquals("FOREIGN_BOOK", foreignEntity.getRelationships().get(0).getForeignEntityName());
      assertEquals(parentEntity.getPrimaryKey(), foreignEntity.getRelationships().get(0).getPrimaryKey());
      assertEquals(foreignEntity.getForeignKeys().get(0), foreignEntity.getRelationships().get(0).getForeignKey());

    }catch(Exception e) {
      e.printStackTrace();
    }
  }

  
  
  @Test

  /*
   *  Two tables Foreign and Parent with a composite primary key imported from the parent table.
   */
    //TODO

  public void test2() {
    
    Connection connection = null;
    
    try {
      
      Class.forName("org.hsqldb.jdbc.JDBCDriver");
      connection = DriverManager.getConnection("jdbc:hsqldb:mem:mydb", "SA", "");

      String parentTableBuilding = "create memory table PARENT_AUTHOR (AUTHOR_ID varchar(256) not null,"+
          " AUTHOR_NAME varchar(256) not null, primary key (AUTHOR_ID))";
      Statement st = connection.createStatement();
      st.execute(parentTableBuilding);


      String foreignTableBuilding = "create memory table FOREIGN_BOOK (BOOK_ID varchar(256) not null, TITLE  varchar(256),"+
          " AUTHOR varchar(256) not null, primary key (BOOK_ID), foreign key (AUTHOR) references PARENT_AUTHOR(AUTHOR_ID))";
      st.execute(foreignTableBuilding);

      this.mapper = new OER2GraphMapper("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem:mydb", "SA", "");
      mapper.buildSourceSchema(this.context);

      
      /*
       *  Testing context information
       */
      
      assertEquals(2, context.getStatistics().totalNumberOfEntities);
      assertEquals(2, context.getStatistics().builtEntities);      
      assertEquals(1, context.getStatistics().detectedRelationships);

      
      /*
       *  Testing built source db schema 
       */
      
      OEntity parentEntity = mapper.getDataBaseSchema().getEntityByName("PARENT_AUTHOR");
      OEntity foreignEntity = mapper.getDataBaseSchema().getEntityByName("FOREIGN_BOOK");
      
      // entities check
      assertEquals(2, mapper.getDataBaseSchema().getEntities().size());
      assertEquals(1, mapper.getDataBaseSchema().getRelationships().size());
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
      assertEquals(1, foreignEntity.getRelationships().size());
      assertEquals(0, parentEntity.getRelationships().size());
      
      assertEquals("PARENT_AUTHOR", foreignEntity.getRelationships().get(0).getParentEntityName());
      assertEquals("FOREIGN_BOOK", foreignEntity.getRelationships().get(0).getForeignEntityName());
      assertEquals(parentEntity.getPrimaryKey(), foreignEntity.getRelationships().get(0).getPrimaryKey());
      assertEquals(foreignEntity.getForeignKeys().get(0), foreignEntity.getRelationships().get(0).getForeignKey());

    }catch(Exception e) {
      e.printStackTrace();
    }
  }
  
}
