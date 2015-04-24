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

package com.orientechnologies.orient.importer.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.importer.model.graphmodel.OAttributeProperties;
import com.orientechnologies.orient.importer.model.graphmodel.OEdgeType;
import com.orientechnologies.orient.importer.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.importer.model.graphmodel.OVertexType;
import com.orientechnologies.orient.importer.persistence.handler.ODriverDataTypeHandler;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 *
 */

public class OGraphModelWriter {



  public OGraphModelWriter() {}


  public boolean writeModelOnOrient(OGraphModel graphModel, ODriverDataTypeHandler handler, String outOrientGraphUri) {
    boolean success = false;

    OrientGraphNoTx orientGraph = new OrientGraphNoTx(outOrientGraphUri);

    try {

      // Building vertex-type

      OLogManager.instance().info(this, "%s", "Building vertex-types.");
      int numberOfVertices = graphModel.getVerticesType().size();

      OrientVertexType newVertexType;
      OAttributeProperties currentAttributeProperties = null;
      List<String> toRemoveAttributes;
      String statement;
      OCommandSQL sqlCommand;
      OType type;

      int iteration = 1;
      for(OVertexType currentVertexType: graphModel.getVerticesType()) {
        OLogManager.instance().info(this, "Building '%s' vertex-type  (%s/%s)...", currentVertexType.getType(),iteration,numberOfVertices);
        newVertexType = orientGraph.createVertexType(currentVertexType.getType());
        toRemoveAttributes = new ArrayList<String>();

        for(String attributeName: currentVertexType.getAttributeName2attributeProperties().keySet()) {
          currentAttributeProperties = currentVertexType.getAttributeName2attributeProperties().get(attributeName);

          type = handler.resolveType(currentAttributeProperties.getAttributeType().toLowerCase(Locale.ENGLISH));
          if(type != null) {
            newVertexType.createProperty(attributeName, type);
            statement = "alter property " + currentVertexType.getType() + "." + attributeName + " custom fromPK = " + currentAttributeProperties.isFromPrimaryKey();
            sqlCommand = new OCommandSQL(statement);
            orientGraph.getRawGraph().command(sqlCommand).execute();
          }
          else{
            toRemoveAttributes.add(attributeName);   
            OLogManager.instance().warn(this, "%s type is not supported, the correspondent property will be dropped.\n", currentAttributeProperties.getAttributeType());
          }
        }

        // property will be dropped both from the POJO schema and from OrientDb schema         
        for(String toRemove: toRemoveAttributes)
          currentVertexType.getAttributeName2attributeProperties().remove(toRemove);

        iteration++;
        OLogManager.instance().info(this, "Vertex-type '%s' built.\n", currentVertexType.getType());
      }

      // Building edge-type

      OLogManager.instance().info(this, "%s", "Building edge-types.");
      int numberOfEdges = graphModel.getEdgesType().size();

      OrientEdgeType newEdgeType;
//      OProperty propIn;
//      OProperty propOut;

      iteration = 1;
      for(OEdgeType currentEdgeType: graphModel.getEdgesType()) {
        OLogManager.instance().info(this, "Building '%s' edge-type  (%s/%s)...", currentEdgeType.getType(),iteration,numberOfEdges);
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

        for(String attributeName: currentEdgeType.getAttributeName2attributeProperties().keySet()) {

          type = handler.resolveType(currentAttributeProperties.getAttributeType().toLowerCase(Locale.ENGLISH));

          if(type != null) {
            newEdgeType.createProperty(attributeName, type);
            statement = "alter property " + currentEdgeType.getType() + "."+ attributeName + " custom fromPK = " + currentAttributeProperties.isFromPrimaryKey();
            sqlCommand = new OCommandSQL(statement);
            orientGraph.getRawGraph().command(sqlCommand).execute();
          }
          else{  
            toRemoveAttributes.add(attributeName);   
            OLogManager.instance().warn(this, "%s type is not supported, the correspondent property will be dropped.\n", currentAttributeProperties.getAttributeType());
          }
        }

        // property will be dropped both from the POJO schema and from OrientDb schema         
        for(String toRemove: toRemoveAttributes)
          currentEdgeType.getAttributeName2attributeProperties().remove(toRemove);

        iteration++;
        OLogManager.instance().info(this, "Edge-type '%s' built.\n", currentEdgeType.getType());
      }


      // Building indexes on properties belonging to the original primary key
      OLogManager.instance().info(this, "%s", "Building indexes on properties belonging to the original primary key.");

      String currentType = null;
      List<String> properties = null;
      iteration = 1;
      for(OVertexType currentVertexType: graphModel.getVerticesType()) {

        currentType = currentVertexType.getType();
        properties = new ArrayList<String>();
        for(String property: currentVertexType.getAttributeName2attributeProperties().keySet()) {
          if(currentVertexType.getAttributeName2attributeProperties().get(property).isFromPrimaryKey()) {
            properties.add(property);
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
        OLogManager.instance().info(this, "Building index for '%s' on %s  (%s/%s)...", currentVertexType.getType(),propertiesList,iteration,numberOfVertices);
        statement = "create index " + currentType + ".pkey" + " on " + currentType + " (" + propertiesList + ") unique_hash_index";
        sqlCommand = new OCommandSQL(statement);
        orientGraph.getRawGraph().command(sqlCommand).execute();
        iteration++;
        OLogManager.instance().info(this, "Index for %s built.\n", currentVertexType.getType());
      }

    }catch(OException e) {
      e.printStackTrace();
    }

    success = true;
    return success;
  }

}
