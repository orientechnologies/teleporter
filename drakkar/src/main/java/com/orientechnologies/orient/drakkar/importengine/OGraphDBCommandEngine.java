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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.drakkar.model.dbschema.OAttribute;
import com.orientechnologies.orient.drakkar.model.dbschema.ORelationship;
import com.orientechnologies.orient.drakkar.model.graphmodel.OVertexType;
import com.orientechnologies.orient.drakkar.nameresolver.ONameResolver;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 *
 */

public class OGraphDBCommandEngine {

  private String graphDBUrl;

  public OGraphDBCommandEngine(String graphDBUrl) {
    this.graphDBUrl = graphDBUrl;
  }


  /**
   * The method perform on the passed OrientGraph a lookup for a OrientVertex starting from a record and from a vertex type.
   * It return the vertex if present, null if not present. 
   * 
   * @param orientGraph
   * @param relation
   * @param currentVertexType
   * @param record
   * @return
   */
  public OrientVertex getVertexByIndexedKey(OrientBaseGraph orientGraph, String[] keys, String[] values, String vertexClassName) {

    /*
     * Graph API throughout index
     */

    OrientVertex vertex = null;

    if(keys.length == 1) {
      //check: the value must be different from null
      if(values[0] != null)
        vertex = (OrientVertex) orientGraph.getVertexByKey(vertexClassName + ".pkey", values[0]);
    }

    else if(keys.length > 1) {

      //check: all values must be different from null
      boolean ok = true;
      for(int i=0; i<values.length; i++) {
        if(values[i] == null) {
          ok = false;
          break;
        }
      }

      if(ok) {
        Iterable<Vertex> iterable = orientGraph.getVertices(vertexClassName, keys, values);
        Iterator<Vertex> iterator = iterable.iterator();
        if(iterator.hasNext())
          vertex = (OrientVertex) iterator.next();
      }
    }

    return vertex;

  }


  /**
   * @param record
   * @throws SQLException 
   */
  public Vertex upsertVisitedVertex(ResultSet record, OVertexType vertexType, ONameResolver nameResolver) throws SQLException {

    OrientGraphFactory factory = new OrientGraphFactory(this.graphDBUrl);
    OrientGraphNoTx orientGraph = factory.getNoTx();
    orientGraph.setStandardElementConstraints(false);
    Map<String,String> properties = new LinkedHashMap<String,String>();

    // building keys and values for the lookup

    List<String> propertiesOfIndex = new LinkedList<String>();

    for(String attribute: vertexType.getAttributeName2attributeProperties().keySet()) {
      // only attribute coming from the primary key are given
      if(vertexType.getAttributeName2attributeProperties().get(attribute).isFromPrimaryKey())
        propertiesOfIndex.add(attribute);
    }

    String[] propertyOfKey = new String[propertiesOfIndex.size()];
    String[] valueOfKey = new String[propertiesOfIndex.size()];

    int cont = 0;
    for(String property: propertiesOfIndex) {
      propertyOfKey[cont] = property;
      valueOfKey[cont] = record.getString(nameResolver.reverseTransformation(property));
      cont++;
    }

    String s = "Keys and values in the lookup (upsertVisitedVertex):\t";
    for(int i=0; i<propertyOfKey.length;i++) {
      s += propertyOfKey[i] + ":" + valueOfKey[i];
    }
    OLogManager.instance().debug(this, "%s", s);

    OrientVertex vertex = this.getVertexByIndexedKey(orientGraph, propertyOfKey, valueOfKey, vertexType.getType());  // !!!

    if(vertex == null) {
      String classAndClusterName = vertexType.getType(); 
      vertex = orientGraph.addVertex(classAndClusterName, classAndClusterName);
    }

    // setting properties to the vertex
    String currentAttributeName = null;
    for(String attributeName : vertexType.getAttributeName2attributeProperties().keySet()) {

      currentAttributeName = record.getString(nameResolver.reverseTransformation(attributeName));
      if(currentAttributeName != null) {
        switch(currentAttributeName) {

        case "t": properties.put(attributeName, "true");
        break;
        case "f": properties.put(attributeName, "false");
        break;
        default: properties.put(attributeName, currentAttributeName);
        break;
        }
      }
      else {
        properties.put(attributeName, currentAttributeName);
      }
    }

    OLogManager.instance().debug(this, "%s", properties);
    vertex.setProperties(properties);
    vertex.save();
    OLogManager.instance().debug(this, "New vertex inserted (all props setted): %s\n", vertex.toString());
    orientGraph.shutdown();

    return vertex;

  }

  /**
   * @param ResultSet foreignRecord: the record correspondent to the current-out-vertex
   * @param ORelationship relation: the relation beetween two entities
   * @param OrientVertex currentOutVertex: the current-out-vertex
   * @param OVertexType currentInVertexType: the type correspondent to the current-in-vertex
   * @param String edgeType: type of the OEdgeType present beetween the two OVertexType, used as label during the insert of the edge in the graph
   * 
   * The method executes insert on reached vertex:
   * - if the vertex is not already reached, it's inserted in the graph and an edge between the out-visited-vertex and the in-reached-vertex is added
   * - if the vertex is already present in the graph no update is performed, neither on reached-vertex neither on the relative edge
   * @throws SQLException 
   */

  public Vertex upsertReachedVertexWithEdge(ResultSet foreignRecord, ORelationship relation, Vertex currentOutVertex, OVertexType currentInVertexType, String edgeType, ONameResolver nameResolver) throws SQLException {

    OrientGraphFactory factory = new OrientGraphFactory(this.graphDBUrl);
    OrientGraphNoTx orientGraph = factory.getNoTx();
    orientGraph.setStandardElementConstraints(false);

    // building keys and values for the lookup 

    String[] propertyOfKey = new String[relation.getForeignKey().getInvolvedAttributes().size()];
    String[] valueOfKey = new String[relation.getForeignKey().getInvolvedAttributes().size()];

    int index = 0;
    for(OAttribute foreignAttribute: relation.getForeignKey().getInvolvedAttributes())  {
      propertyOfKey[index] = nameResolver.resolveVertexProperty(relation.getPrimaryKey().getInvolvedAttributes().get(index).getName());
      valueOfKey[index] = foreignRecord.getString((foreignAttribute.getName()));
      index++;
    }

    String s = "Keys and values in the lookup (upsertReachedVertex):\t";
    for(int i=0; i<propertyOfKey.length;i++) {
      s += propertyOfKey[i] + ":" + valueOfKey[i] + "\t";
    }
    OLogManager.instance().debug(this, "%s", s);



    // new vertex is added only if all the values in the foreign key are different from null
    boolean ok = true;

    for(int i=0; i<valueOfKey.length; i++) {
      if(valueOfKey[i] == null) {
        ok = false;
        break;
      }
    }

    OrientVertex currentInVertex = null;

    // all values are different from null, thus vertex is searched in the graph and in case is added if not found.
    if(ok) {

      currentInVertex = this.getVertexByIndexedKey(orientGraph, propertyOfKey, valueOfKey, currentInVertexType.getType());

      /*
       *  if the vertex is not already present in the graph it's built, set and inserted to the graph,
       *  then the edge beetwen the current-out-vertex and the current-in-vertex is added 
       */
      if(currentInVertex == null) {     

        Map<String,String> partialProperties = new LinkedHashMap<String,String>();

        // for each attribute in the foreign key belonging to the relationship, attribute name and correspondent value are added to a 'properties map'
        for(int i=0; i<propertyOfKey.length; i++) {                
          partialProperties.put(propertyOfKey[i], valueOfKey[i]);
        }

        OLogManager.instance().debug(this, "NEW Reached vertex (id:value) --> %s:%s", Arrays.toString(propertyOfKey),Arrays.toString(valueOfKey));
        String classAndClusterName = currentInVertexType.getType(); 
        currentInVertex = orientGraph.addVertex(classAndClusterName, classAndClusterName);
        currentInVertex.setProperties(partialProperties);
        currentInVertex.save();
        OLogManager.instance().info(this, "New vertex inserted (only pk props setted): %s\n", currentInVertex.toString());

      }

      else {
        OLogManager.instance().debug(this, "NOT NEW Reached vertex, vertex %s:%s already present in the Orient Graph.\n", Arrays.toString(propertyOfKey),Arrays.toString(valueOfKey));
      }

      // create relative edge beetween currentVertex and current reached vertex (just inserted or not)
      // only if it's present in the graph (different from null)
      OrientEdge edge = orientGraph.addEdge(null, currentOutVertex, currentInVertex, edgeType);
      edge.save();
      OLogManager.instance().debug(this, "New edge inserted: %s\n", edge.toString());

    }

    orientGraph.shutdown();

    return currentInVertex;
  }

}
