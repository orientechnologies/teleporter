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

package com.orientechnologies.orient.teleporter.mapper;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.orientechnologies.orient.teleporter.context.OTeleporterContext;
import com.orientechnologies.orient.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.orient.teleporter.model.dbschema.ODataBaseSchema;

/**
 * Extends OER2GraphMapper thus manages the source DB schema and the destination graph model with their correspondences.
 * Unlike the superclass, this class builds the source DB schema starting from JPA's XML configuration file.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OJPA2GraphMapper extends OER2GraphMapper {
  
  private String xmlPath;
  
  public OJPA2GraphMapper(String driver, String uri, String username, String password, String xmlPath, List<String> includedTables, List<String> excludedTables) {
    super(driver, uri, username, password, includedTables, excludedTables);
    this.xmlPath = xmlPath;
  }
  
  @Override
  public void buildSourceSchema(OTeleporterContext context) {

    Connection connection = null;
    OTeleporterStatistics statistics = context.getStatistics();
    statistics.startWork1Time = new Date();
    statistics.runningStepNumber = 1;
    statistics.notifyListeners();

    try {

      connection = this.dbSourceConnection.getConnection(context);
      DatabaseMetaData databaseMetaData = connection.getMetaData();

      /*
       *  General DB Info
       */

      int majorVersion = databaseMetaData.getDatabaseMajorVersion();
      int minorVersion = databaseMetaData.getDatabaseMinorVersion();
      int driverMajorVersion = databaseMetaData.getDriverMajorVersion();
      int driverMinorVersion = databaseMetaData.getDriverMinorVersion();
      String productName = databaseMetaData.getDatabaseProductName();
      String productVersion = databaseMetaData.getDatabaseProductVersion();

      this.dataBaseSchema = new ODataBaseSchema(majorVersion, minorVersion, driverMajorVersion, driverMinorVersion, productName, productVersion);
      
      // XML parsing and DOM building
      
      File xmlFile = new File(this.xmlPath);
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document dom = dBuilder.parse(xmlFile);
      
      NodeList entities = dom.getElementsByTagName("class");
      
      for(int i=0; i<entities.getLength(); i++) {

        Element currentEntityElement = (Element) entities.item(i);
        if(super.dataBaseSchema.getEntityByName(currentEntityElement.getAttribute("table")).getPrimaryKey() == null) {
          
        }
        
      }

      
      /*
       *  Entity building
       */
      
      // TODO
      
      
      
      /*
       *  Building relationships
       */
      
      // TODO

      
      
      
      
      
    }catch(SQLException e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }catch(Exception e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }finally {
      try {
        if(connection != null) {
          connection.close();
        }
      }catch(SQLException e) {
        context.getOutputManager().error(e.getMessage());
        Writer writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        context.getOutputManager().debug(writer.toString());
      }
    }

    try {
      if(connection.isClosed())
        context.getOutputManager().debug("Connection to DB closed.\n");
      else {
        statistics.warningMessages.add("Connection to DB not closed.");
      }      
    }catch(SQLException e) {
      context.getOutputManager().error(e.getMessage());
      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      context.getOutputManager().debug(writer.toString());
    }
    
  }
  

}
