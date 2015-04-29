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

package com.orientechnologies.orient.drakkar.importengine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.drakkar.persistence.util.ODataSource;

/**
 * Implementation of ODataSourceQueryEngine. It executes the necessary queries for the source DB records fetching.
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class ODBQueryEngine implements ODataSourceQueryEngine {

  private ODataSource dataSource;
  private Connection dbConnection;
  private PreparedStatement statement;

  public ODBQueryEngine(String driver, String uri, String username, String password) {
    this.dataSource =  new ODataSource(driver, uri, username, password);
  }


  public ResultSet getRecordsByEntity(String entityName) {

    ResultSet results = null;
    this.dbConnection = null;
    this.statement = null;
    String query = "select * from " + entityName;

    try {
      
      this.dbConnection = dataSource.getConnection();
      this.statement = dbConnection.prepareStatement(query);
      results = statement.executeQuery();      
      
    }catch(SQLException e) {
      OLogManager.instance().debug(this, "%s", e.getMessage());
      e.printStackTrace();
    }
    return results;

  }


  /**
   * UNUSED, DELETE?
   * @return reachable record parameters (parentEntityName, parentEntityAttributeValue) in order to build the vertex:
   * - parent entity name
   * - parent entity attribute --> name of the attribute on which join is performed
   * - parent entity attribute value --> value on which join is performed
   */
//  public String[] getRecordByConnectedRecord(ResultSet record, ORelationship relation, OEntity startingEntity) {
//
//    ResultSet result;
//    Connection dbConnection = null;
//    PreparedStatement statement = null;
//    String foreignEntityName = relation.getForeignEntityName();
//    String parentEntityName = relation.getParentEntityName();
//    String foreignEntityAttribute = relation.getForeignEntityAttribute();
//    String parentEntityAttribute = relation.getParentEntityAttribute(); 
//   
//    
//    /*
//     * select startingRecord.<foreignEntityAttribute> from 
//     * (select * from <foreignEntityName> where <p.key1> = <value> and <p.key2> = <value>) as startingRecord 
//     * join <parentEntityName> as arrivalRecord on <foreignEntityAttribute> = <parentEntityAttribute>
//     */
//
//    String parentEntityAttributeValue = null; 
//    String whereConditions = "";
//
//    try {
//
//      int length = startingEntity.getPrimaryKey().getInvolvedAttributes().size();
//      int cont = 1;
//
//      for(OAttribute attribute: startingEntity.getPrimaryKey().getInvolvedAttributes()) {
//        if(cont == length)
//          whereConditions += attribute.getName() + " = " + record.getString(attribute.getName());
//        else
//          whereConditions += attribute.getName() + " = " + record.getString(attribute.getName()) + " and ";
//      }
//
//      String query = "select arrivalRecord." + parentEntityAttribute + " from\n" + 
//          "(select * from " + foreignEntityName + " where " + whereConditions + ") as startingRecord\n" + 
//          "join " + parentEntityName + " as arrivalRecord on " + "startingRecord." + foreignEntityAttribute + " = " + "arrivalRecord." + parentEntityAttribute;
//
//      OLogManager.instance().debug(this, "%s\n", query);
//      dbConnection = dataSource.getConnection();      
//      statement = dbConnection.prepareStatement(query);
//      result = statement.executeQuery();  
//      result.next();
//      parentEntityAttributeValue = result.getString(1);
//
//    }catch(SQLException e) {
//      OLogManager.instance().debug(this, "%s\n", e.getMessage());
//      e.printStackTrace();
//    }finally {
//      try {
//        if(dbConnection != null) 
//          dbConnection.close();
//        if(statement != null) 
//          statement.close();
//
//      }catch(SQLException e) {
//        OLogManager.instance().debug(this, "%s\n", e.getMessage());
//        e.printStackTrace();
//      }
//    }
//
//    String[] parameters = new String[3];
//    parameters[0] = parentEntityAttribute;
//    parameters[1] = parentEntityAttributeValue;
//
//    return parameters;
//  }
  
  
  public void closeAll() {
    
    try {
      if(this.dbConnection != null) 
        this.dbConnection.close();
      if(this.statement != null) 
        this.statement.close();

    }catch(SQLException e) {
      OLogManager.instance().debug(this, "%s", e.getMessage());
      e.printStackTrace();
    }
    
  }




}
