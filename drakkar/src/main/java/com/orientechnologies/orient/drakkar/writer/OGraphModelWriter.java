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

package com.orientechnologies.orient.drakkar.writer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.ODrakkarStatistics;
import com.orientechnologies.orient.drakkar.model.graphmodel.OEdgeType;
import com.orientechnologies.orient.drakkar.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.drakkar.model.graphmodel.OProperty;
import com.orientechnologies.orient.drakkar.model.graphmodel.OVertexType;
import com.orientechnologies.orient.drakkar.persistence.handler.ODriverDataTypeHandler;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Writer that has the responsibility to write the model of the destination Orient Graph
 * on OrientDB as an OrientDB Schema.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OGraphModelWriter {



  public OGraphModelWriter() {}


  public boolean writeModelOnOrient(OGraphModel graphModel, ODriverDataTypeHandler handler, String outOrientGraphUri, ODrakkarContext context) {
    boolean success = false;

    OrientGraphNoTx orientGraph = new OrientGraphNoTx(outOrientGraphUri);
    ODrakkarStatistics statistics = context.getStatistics();
    statistics.startWork3Time = new Date();
    statistics.runningStepNumber = 3;

    try {

      // Writing vertex-type

      context.getOutputManager().debug("Writing vertex-types on OrientDB Schema...");
      int numberOfVertices = graphModel.getVerticesType().size();
      statistics.totalNumberOfVertexType = numberOfVertices;

      OrientVertexType newVertexType;
//      OProperty currentProperty = null;
      List<String> toRemoveAttributes;
      String statement;
      OCommandSQL sqlCommand;
      OType type;

      int iteration = 1;
      for(OVertexType currentVertexType: graphModel.getVerticesType()) {
        context.getOutputManager().debug("Writing '" + currentVertexType.getType() + "' vertex-type  (" + iteration + "/" + numberOfVertices + ")...");
        newVertexType = orientGraph.createVertexType(currentVertexType.getType());
        toRemoveAttributes = new ArrayList<String>();

        for(OProperty currentProperty: currentVertexType.getProperties()) {
          
          type = handler.resolveType(currentProperty.getPropertyType().toLowerCase(Locale.ENGLISH), context);
          if(type != null) {
            newVertexType.createProperty(currentProperty.getName(), type);
            statement = "alter property " + currentVertexType.getType() + "." + currentProperty.getName() + " custom fromPK = " + currentProperty.isFromPrimaryKey();
            sqlCommand = new OCommandSQL(statement);
            orientGraph.getRawGraph().command(sqlCommand).execute();
          }
          else{
            toRemoveAttributes.add(currentProperty.getName());   
            statistics.warningMessages.add(currentProperty.getPropertyType() + " type is not supported, the correspondent property will be dropped.");
          }
        }

        // property will be dropped both from the POJO schema and from OrientDb schema         
        for(String toRemove: toRemoveAttributes)
          currentVertexType.removePropertyByName(toRemove);

        iteration++;
        context.getOutputManager().debug("Vertex-type '" + currentVertexType.getType() + "' wrote.\n");
        statistics.wroteVertexType++;
      }

      // Writing edge-type

      context.getOutputManager().debug("Writing edge-types on OrientDB Schema...");
      int numberOfEdges = graphModel.getEdgesType().size();
      statistics.totalNumberOfEdgeType = numberOfEdges;

      OrientEdgeType newEdgeType;

      iteration = 1;
      for(OEdgeType currentEdgeType: graphModel.getEdgesType()) {
        context.getOutputManager().debug("Writing '" + currentEdgeType.getType() + "' edge-type  (" + iteration + "/" + numberOfEdges + ")...");
        newEdgeType = orientGraph.createEdgeType(currentEdgeType.getType());

//        // setting in-vertex-type
//
//        propIn = newEdgeType.createProperty("in", OType.LINK);
//        propIn.setLinkedClass(orientGraph.getVertexType(currentEdgeType.getInVertexType().getType()));
//
//        // setting out-vertex-type
//        propOut = newEdgeType.createProperty("out", OType.LINK);
//        propOut.setLinkedClass(orientGraph.getVertexType(currentEdgeType.getOutVertexType().getType()));

        toRemoveAttributes = new ArrayList<String>();

        for(OProperty currentProperty: currentEdgeType.getProperties()) {

          type = handler.resolveType(currentProperty.getPropertyType().toLowerCase(Locale.ENGLISH), context);

          if(type != null) {
            newEdgeType.createProperty(currentProperty.getName(), type);
            statement = "alter property " + currentEdgeType.getType() + "."+ currentProperty.getName() + " custom fromPK = " + currentProperty.isFromPrimaryKey();
            sqlCommand = new OCommandSQL(statement);
            orientGraph.getRawGraph().command(sqlCommand).execute();
          }
          else {  
            toRemoveAttributes.add(currentProperty.getName());   
            statistics.warningMessages.add(currentProperty.getPropertyType() + " type is not supported, the correspondent property will be dropped.");
          }
        }

        // property will be dropped both from the POJO schema and from OrientDb schema         
        for(String toRemove: toRemoveAttributes)
          currentEdgeType.removePropertyByName(toRemove);

        iteration++;
        context.getOutputManager().debug("Edge-type '" + currentEdgeType.getType() + "' wrote.\n");
        statistics.wroteEdgeType++;
      }


      // Writing indexes on properties belonging to the original primary key
      context.getOutputManager().debug("Building indexes on properties belonging to the original primary key...");
      statistics.totalNumberOfIndices = numberOfVertices;

      String currentType = null;
      List<String> properties = null;
      iteration = 1;
      for(OVertexType currentVertexType: graphModel.getVerticesType()) {

        currentType = currentVertexType.getType();
        properties = new ArrayList<String>();
        for(OProperty currentProperty: currentVertexType.getProperties()) {
          if(currentProperty.isFromPrimaryKey()) {
            properties.add(currentProperty.getName());
          }
        }

        String propertiesList = "";
        int j = 0;
        for(String property: properties) {
          if(j == properties.size()-1) 
            propertiesList += property;
          else
            propertiesList += property + ",";
          j++;
        }
        context.getOutputManager().debug("Building index for '" + currentVertexType.getType() + "' on " + propertiesList + "  (" + iteration + "/" + numberOfVertices + ")...");
        statement = "create index " + currentType + ".pkey" + " on " + currentType + " (" + propertiesList + ") unique_hash_index";
        sqlCommand = new OCommandSQL(statement);
        orientGraph.getRawGraph().command(sqlCommand).execute();
        iteration++;
        context.getOutputManager().debug("Index for " + currentVertexType.getType() + " built.\n");
        statistics.wroteIndices++;
      }

    }catch(OException e) {
      e.printStackTrace();
    }    
    statistics.notifyListeners();
    statistics.runningStepNumber = -1;

    success = true;
    return success;
  }

}
