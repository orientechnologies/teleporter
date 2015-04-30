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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.factory.ODataTypeHandlerFactory;
import com.orientechnologies.orient.drakkar.factory.ONameResolverFactory;
import com.orientechnologies.orient.drakkar.importengine.ODB2GraphImportEngine;
import com.orientechnologies.orient.drakkar.mapper.OER2GraphMapper;
import com.orientechnologies.orient.drakkar.mapper.OSource2GraphMapper;
import com.orientechnologies.orient.drakkar.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.drakkar.nameresolver.ONameResolver;
import com.orientechnologies.orient.drakkar.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.orient.drakkar.ui.OProgressMonitor;
import com.orientechnologies.orient.drakkar.util.OTimeFormatHandler;
import com.orientechnologies.orient.drakkar.writer.OGraphModelWriter;

/**
 * A strategy that performs a "naive" import of the data source. The data source schema is
 * translated semi-directly in a correspondent and coherent graph model.
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 * 
 */

public class ONaiveImportStrategy implements OImportStrategy {

  //Name Resolver
  private ONameResolver nameResolver;

  public ONaiveImportStrategy() {}

  public void executeStrategy(String driver, String uri, String username, String password, String outOrientGraphUri, String nameResolverConvention) {	
    
    // Context and Progress Monitor initialization
    ODrakkarContext context = new ODrakkarContext();
    OProgressMonitor progressMonitor = new OProgressMonitor();
    progressMonitor.initialize(context.getStatistics());
    
    ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
    ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention);
    this.setNameResolver(nameResolver);
    OSource2GraphMapper mapper = this.createSchemaMapper(driver, uri, username, password, outOrientGraphUri, nameResolver, context);
    this.executeImport(driver, uri, username, password, outOrientGraphUri, mapper, nameResolver, context);
  }

  public OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, ONameResolver nameResolver, ODrakkarContext context) {

    OSource2GraphMapper mapper = new OER2GraphMapper(driver, uri, username, password);

    // DataBase schema building
    mapper.buildSourceSchema(context);

    OLogManager.instance().debug(this, "%s\n", ((OER2GraphMapper)mapper).getDataBaseSchema().toString());
//        System.out.println(((OER2GraphMapper)mapper).getDataBaseSchema().toString());

    // Graph model building
    mapper.buildGraphModel(nameResolver, context);

    OLogManager.instance().debug(this, "%s\n", ((OER2GraphMapper)mapper).getGraphModel().toString());
//        System.out.println(((OER2GraphMapper)mapper).getGraphModel().toString());

    // Saving schema on Orient
    ODataTypeHandlerFactory factory = new ODataTypeHandlerFactory();
    ODriverDataTypeHandler handler = factory.buildDataTypeHandler(driver);
    OGraphModelWriter graphModelWriter = new OGraphModelWriter();  
    OGraphModel graphModel = ((OER2GraphMapper)mapper).getGraphModel();
    boolean success = graphModelWriter.writeModelOnOrient(graphModel, handler, outOrientGraphUri, context);

    if(!success) {
      OLogManager.instance().error(this, "Writing not complete. Something's gone wrong.\n", (Object[]) null);
      System.exit(0);
    }

    return mapper;
  }


  public void executeImport(String driver, String uri, String username, String password, String outOrientGraphUri, OSource2GraphMapper mapper,  ONameResolver nameResolver, ODrakkarContext context) {

    ODB2GraphImportEngine importEngine = new ODB2GraphImportEngine();

    try {
      importEngine.executeImport(driver, uri, username, password, outOrientGraphUri, mapper, nameResolver, context);
    }catch(Exception e){
      e.printStackTrace();
    }
  }


  public ONameResolver getNameResolver() {
    return this.nameResolver;
  }


  public void setNameResolver(ONameResolver nameResolver) {
    this.nameResolver = nameResolver;
  }

}
