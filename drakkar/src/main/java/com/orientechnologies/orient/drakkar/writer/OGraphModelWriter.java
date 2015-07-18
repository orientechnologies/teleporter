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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.ODrakkarStatistics;
import com.orientechnologies.orient.drakkar.model.graphmodel.OEdgeType;
import com.orientechnologies.orient.drakkar.model.graphmodel.OElementType;
import com.orientechnologies.orient.drakkar.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.drakkar.model.graphmodel.OModelProperty;
import com.orientechnologies.orient.drakkar.model.graphmodel.OVertexType;
import com.orientechnologies.orient.drakkar.persistence.handler.ODriverDataTypeHandler;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientElementType;
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

      /*
       * Writing vertex-type
       */

      context.getOutputManager().debug("Writing vertex-types on OrientDB Schema...");
      int numberOfVertices = graphModel.getVerticesType().size();
      statistics.totalNumberOfVertexType = numberOfVertices;

      OrientVertexType newVertexType;
      String statement;
      OCommandSQL sqlCommand;
      OType type;
      Iterator<OModelProperty> it = null;

      int iteration = 1;
      for(OVertexType currentVertexType: graphModel.getVerticesType()) {
        context.getOutputManager().debug("Writing '" + currentVertexType.getName() + "' vertex-type  (" + iteration + "/" + numberOfVertices + ")...");

        // check if vertex type is already present in the orient schema
        newVertexType = orientGraph.getVertexType(currentVertexType.getName());

        if(newVertexType == null) {

          // inheritance case
          if(currentVertexType.getParentType() != null)
            newVertexType = orientGraph.createVertexType(currentVertexType.getName(), currentVertexType.getParentType().getName());
          else
            newVertexType = orientGraph.createVertexType(currentVertexType.getName());

          OModelProperty currentProperty = null;
          it = currentVertexType.getProperties().iterator();
          while(it.hasNext()) {
            currentProperty = it.next();
            type = handler.resolveType(currentProperty.getPropertyType().toLowerCase(Locale.ENGLISH), context);
            if(type != null) {
              String propertyName = currentProperty.getName();
              newVertexType.createProperty(propertyName, type);
            }
            else {
              it.remove();
              statistics.warningMessages.add(currentProperty.getPropertyType() + " type is not supported, the correspondent property will be dropped.");
            }
          }
          context.getOutputManager().debug("Vertex-type '" + currentVertexType.getName() + "' wrote.\n");
        }
        else  {
          boolean updated = this.checkAndUpdateClass(orientGraph, currentVertexType, handler, context);

          if(updated) {
            context.getOutputManager().debug("Vertex-type '" + currentVertexType.getName() + "' updated.\n");
          }
          else {
            context.getOutputManager().debug("Vertex-type '" + currentVertexType.getName() + "' already present in the Orient schema.\n");
          }
        }

        iteration++;
        statistics.wroteVertexType++;
      }

      /*
       * Writing edge-type
       */

      context.getOutputManager().debug("Writing edge-types on OrientDB Schema...");
      int numberOfEdges = graphModel.getEdgesType().size();
      statistics.totalNumberOfEdgeType = numberOfEdges;

      OrientEdgeType newEdgeType;

      iteration = 1;
      for(OEdgeType currentEdgeType: graphModel.getEdgesType()) {
        context.getOutputManager().debug("Writing '" + currentEdgeType.getName() + "' edge-type  (" + iteration + "/" + numberOfEdges + ")...");

        // check if edge type is already present in the orient schema
        newEdgeType = orientGraph.getEdgeType(currentEdgeType.getName());

        if(newEdgeType == null) {
          newEdgeType = orientGraph.createEdgeType(currentEdgeType.getName());
          OModelProperty currentProperty = null;
          it = currentEdgeType.getProperties().iterator();
          while(it.hasNext()) {
            currentProperty = it.next();
            type = handler.resolveType(currentProperty.getPropertyType().toLowerCase(Locale.ENGLISH), context);

            if(type != null) {
              newEdgeType.createProperty(currentProperty.getName(), type);
            }
            else {  
              it.remove();
              statistics.warningMessages.add(currentProperty.getPropertyType() + " type is not supported, the correspondent property will be dropped.");
            }
          }
          context.getOutputManager().debug("Edge-type '" + currentEdgeType.getName() + "' wrote.\n");
        }
        else {
          boolean updated = this.checkAndUpdateClass(orientGraph, currentEdgeType, handler, context);

          if(updated) {
            context.getOutputManager().debug("Edge-type '" + currentEdgeType.getName() + "' updated.\n");
          }
          else {
            context.getOutputManager().debug("Edge-type '" + currentEdgeType.getName() + "' already present in the Orient schema.\n");
          }
        }
        iteration++;
        statistics.wroteEdgeType++;
      }

      /*
       *  Writing indexes on properties belonging to the original primary key
       */

      context.getOutputManager().debug("Building indexes on properties belonging to the original primary key...");
      statistics.totalNumberOfIndices = numberOfVertices;

      String currentType = null;
      List<String> properties = null;
      iteration = 1;
      boolean isPresent;
      for(OVertexType currentVertexType: graphModel.getVerticesType()) {

        currentType = currentVertexType.getName();
        properties = new ArrayList<String>();
        for(OModelProperty currentProperty: currentVertexType.getProperties()) {
          if(currentProperty.isFromPrimaryKey()) {
            properties.add(currentProperty.getName());
          }
        }

        // check if edge type is already present in the orient schema
        isPresent = orientGraph.getRawGraph().getMetadata().getIndexManager().areIndexed(currentVertexType.getName(), properties);

        if(!isPresent) {

          String propertiesList = "";
          int j = 0;
          for(String property: properties) {
            if(j == properties.size()-1) 
              propertiesList += property;
            else
              propertiesList += property + ",";
            j++;
          }

          if(!propertiesList.isEmpty()) {
            context.getOutputManager().debug("Building index for '" + currentVertexType.getName() + "' on " + propertiesList + "  (" + iteration + "/" + numberOfVertices + ")...");
            statement = "create index " + currentType + ".pkey" + " on " + currentType + " (" + propertiesList + ") unique_hash_index";
            sqlCommand = new OCommandSQL(statement);
            orientGraph.getRawGraph().command(sqlCommand).execute();
            context.getOutputManager().debug("Index for " + currentVertexType.getName() + " built.\n");
          }
          else {
            context.getStatistics().warningMessages.add("The table '" + currentVertexType.getName() + "' has not primary key constraints defined in the db schema,"
                + " thus the correspondent Class Vertex in Orient will not have a default index on the original primary key.");
          }
        }
        else {
          context.getOutputManager().debug("Index for " + currentVertexType.getName() + " already present in the Orient schema.\n");
        }
        iteration++;
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


  /**
   * 
   * 
   * @param orientGraph
   * @param currentElementType
   * @return
   */
  private boolean checkAndUpdateClass(OrientGraphNoTx orientGraph, OElementType currentElementType, ODriverDataTypeHandler handler, ODrakkarContext context) {

    boolean updated = false;

    OrientElementType orientElementType = null;

    if(currentElementType instanceof OVertexType) {
      orientElementType = orientGraph.getVertexType(currentElementType.getName());
    }
    else if(currentElementType instanceof OEdgeType) {
      orientElementType = orientGraph.getEdgeType(currentElementType.getName());
    }
    else {
      context.getOutputManager().error("Fatal error: current element type '" + currentElementType + "' is not instance neither of Vertex Type neither of EdgeType");
      System.exit(0);
    }

    OProperty orientSchemaProperty;
    OType type;

    // check from model properties
    Iterator<OModelProperty> it1 = currentElementType.getProperties().iterator();
    OModelProperty currentProperty;
    while(it1.hasNext()) {
      currentProperty = it1.next();
      orientSchemaProperty = orientElementType.getProperty(currentProperty.getName());
      type = handler.resolveType(currentProperty.getPropertyType().toLowerCase(Locale.ENGLISH), context);

      if(orientSchemaProperty != null) {
        // property present in orientdb schema, check if is it equal (type check), in case it's modified

        // if types are not equal the property will be dropped and added again with the correct type
        if(!currentProperty.getPropertyType().equalsIgnoreCase(type.toString())) {
          orientElementType.dropProperty(currentProperty.getName());
          orientElementType.createProperty(currentProperty.getName(), type);
          updated = true;
        }
      }
      else {
        // property not present in orientdb schema, then it's added (if type allows it)
        orientElementType.createProperty(currentProperty.getName(), type);
        updated = true;
      }
    }

    // check from orientdb schema properties
    OProperty orientSchemaProperty2;
    Iterator<OProperty> it2 = orientElementType.declaredProperties().iterator();
    while(it2.hasNext()) {
      orientSchemaProperty2 = it2.next();
      // if the property is not present in the model vertex type, then is dropped
      if(currentElementType.getPropertyByName(orientSchemaProperty2.getName()) == null) {
        orientElementType.dropProperty(orientSchemaProperty2.getName());
        updated = true;
      }
    }
    return updated;
  }

}
