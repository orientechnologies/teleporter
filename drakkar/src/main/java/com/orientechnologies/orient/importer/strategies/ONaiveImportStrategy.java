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

package com.orientechnologies.orient.importer.strategies;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.importer.factory.ODataTypeHandlerFactory;
import com.orientechnologies.orient.importer.factory.ONameResolverFactory;
import com.orientechnologies.orient.importer.importengine.ODB2GraphImportEngine;
import com.orientechnologies.orient.importer.mapper.OER2GraphMapper;
import com.orientechnologies.orient.importer.mapper.OSource2GraphMapper;
import com.orientechnologies.orient.importer.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.importer.nameresolver.ONameResolver;
import com.orientechnologies.orient.importer.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.orient.importer.writer.OGraphModelWriter;

/**
 * <CLASS DESCRIPTION>
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 * 
 */

public class ONaiveImportStrategy implements OImportStrategy {

  //Name Resolver
  private ONameResolver nameResolver;

  public ONaiveImportStrategy() {}

  public void executeStrategy(String driver, String uri, String username, String password, String outOrientGraphUri, String nameResolverConvention) {		
    ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
    ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention);
    this.setNameResolver(nameResolver);
    OSource2GraphMapper mapper = this.createSchemaMapper(driver, uri, username, password, outOrientGraphUri, nameResolver);
    this.executeImport(driver, uri, username, password, outOrientGraphUri, mapper, nameResolver);
  }

  public OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, ONameResolver nameResolver) {

    OSource2GraphMapper mapper = new OER2GraphMapper(driver, uri, username, password);
    
    // DataBase schema building
    OLogManager.instance().info(this, "Building the DataBase schema...\n", (Object[]) null);
    Date start = new Date();
    mapper.buildSourceSchema();
    Date end = new Date();
    OLogManager.instance().info(this, "DataBase schema building complete.\n", (Object[]) null);
    
    String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime()), TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())
        - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime())), TimeUnit.MILLISECONDS.toSeconds(end.getTime()-start.getTime())
        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())));
    OLogManager.instance().info(this, "Elapsed time: %s s.\n", hms);
    
    OLogManager.instance().debug(this, "%s\n", ((OER2GraphMapper)mapper).getDataBaseSchema().toString());


    // Graph model building
    OLogManager.instance().info(this, "Building the graph model...\n", (Object[]) null);
    start = new Date();
    mapper.buildGraphModel(nameResolver);
    end = new Date();
    OLogManager.instance().info(this, "Graph model building complete.\n", (Object[]) null);
    
    hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime()), TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())
        - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime())), TimeUnit.MILLISECONDS.toSeconds(end.getTime()-start.getTime())
        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())));
    OLogManager.instance().info(this, "Elapsed time: %s s.\n", hms);
    
    OLogManager.instance().debug(this, "%s\n", ((OER2GraphMapper)mapper).getGraphModel().toString());


    // Saving schema on Orient
    ODataTypeHandlerFactory factory = new ODataTypeHandlerFactory();
    ODriverDataTypeHandler handler = factory.buildDataTypeHandler(driver);
    String outDbRootDirUri = outOrientGraphUri.substring(outOrientGraphUri.indexOf(':'), outOrientGraphUri.lastIndexOf('/')+1);
    OLogManager.instance().info(this, "OrientGraph schema writing at %s...\n", outDbRootDirUri);
    start = new Date();
    OGraphModelWriter graphModelWriter = new OGraphModelWriter();  
    OGraphModel graphModel = ((OER2GraphMapper)mapper).getGraphModel();
    boolean success = graphModelWriter.writeModelOnOrient(graphModel, handler, outOrientGraphUri);
    end = new Date();

    if(success) {
      OLogManager.instance().info(this, "Writing complete.\n", (Object[]) null);
    }
    else {
      OLogManager.instance().error(this, "Writing not complete. Something's gone wrong.\n", (Object[]) null);

    }
    
    hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime()), TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())
        - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime())), TimeUnit.MILLISECONDS.toSeconds(end.getTime()-start.getTime())
        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())));
    OLogManager.instance().info(this, "Elapsed time: %s s.\n", (end.getTime() - start.getTime())/1000);

    
    return mapper;
  }

  
  public void executeImport(String driver, String uri, String username, String password, String outOrientGraphUri, OSource2GraphMapper mapper,  ONameResolver nameResolver) {

    OLogManager.instance().info(this, "Importing phase in progress...\n", (Object[]) null);
    Date start = new Date();
    ODB2GraphImportEngine importEngine = new ODB2GraphImportEngine();

    try {
      importEngine.executeImport(driver, uri, username, password, outOrientGraphUri, mapper, nameResolver);
      Date end = new Date();
      OLogManager.instance().info(this, "Importing phase completed.\n", (Object[]) null);
      
      String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime()), TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())
          - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime())), TimeUnit.MILLISECONDS.toSeconds(end.getTime()-start.getTime())
          - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())));      
      OLogManager.instance().info(this, "Elapsed time: %s s.\n", hms);
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
