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

package com.orientechnologies.orient.drakkar.strategy;

import java.util.Date;
import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.factory.ODataTypeHandlerFactory;
import com.orientechnologies.orient.drakkar.factory.ONameResolverFactory;
import com.orientechnologies.orient.drakkar.importengine.ODB2GraphImportEngine;
import com.orientechnologies.orient.drakkar.mapper.OER2GraphMapper;
import com.orientechnologies.orient.drakkar.mapper.OSource2GraphMapper;
import com.orientechnologies.orient.drakkar.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.drakkar.nameresolver.ONameResolver;
import com.orientechnologies.orient.drakkar.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.orient.drakkar.util.OTimeFormatHandler;
import com.orientechnologies.orient.drakkar.writer.OGraphModelWriter;

/**
 * A strategy that performs a "naive" import of the data source. The data source schema is
 * translated semi-directly in a correspondent and coherent graph model.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ONaiveImportStrategy implements OImportStrategy {


  public ONaiveImportStrategy() {}

  public void executeStrategy(String driver, String uri, String username, String password, String outOrientGraphUri, String nameResolverConvention, ODrakkarContext context) {	

    Date globalStart = new Date(); 

    context.getStatistics().notifyListeners();

    // Step 1,2,3
    ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
    ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention, context);
    OSource2GraphMapper mapper = this.createSchemaMapper(driver, uri, username, password, outOrientGraphUri, nameResolver, context);

    // Step 4
    this.executeImport(driver, uri, username, password, outOrientGraphUri, mapper, context);
    context.getStatistics().runningStepNumber = -1;

    Date globalEnd = new Date();

    context.getOutputManager().info("\n\nImporting Complete in " + OTimeFormatHandler.getHMSFormat(globalStart, globalEnd) + " !");
    context.getOutputManager().info(context.getStatistics().toString());

  }

  public OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, ONameResolver nameResolver, ODrakkarContext context) {

    OSource2GraphMapper mapper = new OER2GraphMapper(driver, uri, username, password);

    // DataBase schema building
    mapper.buildSourceSchema(context);
    context.getOutputManager().info("");
    context.getOutputManager().debug(((OER2GraphMapper)mapper).getDataBaseSchema().toString() + "\n");

    // Graph model building
    mapper.buildGraphModel(nameResolver, context);
    context.getOutputManager().info("");
    context.getOutputManager().debug(((OER2GraphMapper)mapper).getGraphModel().toString() + "\n");

    // Saving schema on Orient
    ODataTypeHandlerFactory factory = new ODataTypeHandlerFactory();
    ODriverDataTypeHandler handler = factory.buildDataTypeHandler(driver, context);
    OGraphModelWriter graphModelWriter = new OGraphModelWriter();  
    OGraphModel graphModel = ((OER2GraphMapper)mapper).getGraphModel();
    boolean success = graphModelWriter.writeModelOnOrient(graphModel, handler, outOrientGraphUri, context);
    if(!success) {
      context.getOutputManager().error("Writing not complete. Something's gone wrong.\n");
      System.exit(0);
    }
    context.getOutputManager().info("OrientDB Schema writing complete.");

    return mapper;
  }


  public void executeImport(String driver, String uri, String username, String password, String outOrientGraphUri, OSource2GraphMapper mapper,  ODrakkarContext context) {

    ODB2GraphImportEngine importEngine = new ODB2GraphImportEngine();

    try {
      importEngine.executeImport(driver, uri, username, password, outOrientGraphUri, mapper, context);
      context.getOutputManager().info("");
    }catch(Exception e){
      e.printStackTrace();
    }
  }

}
