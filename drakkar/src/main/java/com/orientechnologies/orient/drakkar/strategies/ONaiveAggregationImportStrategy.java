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

package com.orientechnologies.orient.drakkar.strategies;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.drakkar.factory.ODataTypeHandlerFactory;
import com.orientechnologies.orient.drakkar.mapper.OER2GraphMapper;
import com.orientechnologies.orient.drakkar.mapper.OSource2GraphMapper;
import com.orientechnologies.orient.drakkar.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.drakkar.nameresolver.ONameResolver;
import com.orientechnologies.orient.drakkar.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.orient.drakkar.writer.OGraphModelWriter;

/**
 * A strategy that performs a "naive" import of the source data. The source data schema is
 * translated semi-directly in a correspondent and coherent graph model using an aggregation 
 * policy on the junction tables of dimension equals to 2.
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 * 
 */

public class ONaiveAggregationImportStrategy extends ONaiveImportStrategy {	

  public ONaiveAggregationImportStrategy() {}


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



    // Many-to-Many aggregation
    OLogManager.instance().info(this, "Many-To-Many aggregation in progress...\n", (Object[]) null);
    start = new Date();
    ((OER2GraphMapper)mapper).Many2ManyAggregation();
    end = new Date();
    OLogManager.instance().info(this, "'Junction-Entity' aggregation complete.\n", (Object[]) null);

    hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime()), TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())
        - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime())), TimeUnit.MILLISECONDS.toSeconds(end.getTime()-start.getTime())
        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())));
    OLogManager.instance().info(this, "Elapsed time: %s ms.\n", hms);

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

}
