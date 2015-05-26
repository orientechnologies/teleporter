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
import java.util.Locale;
import java.util.Map;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.mapper.OAggregatorEdge;
import com.orientechnologies.orient.drakkar.model.dbschema.OAttribute;
import com.orientechnologies.orient.drakkar.model.dbschema.OEntity;
import com.orientechnologies.orient.drakkar.model.dbschema.ORelationship;
import com.orientechnologies.orient.drakkar.model.graphmodel.OModelProperty;
import com.orientechnologies.orient.drakkar.model.graphmodel.OVertexType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Executes the necessary operations of insert and upsert for the destination Orient DB populating.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
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

    OrientVertex vertex = null;
    Iterator<Vertex> iterator = orientGraph.getVertices(vertexClassName, keys, values).iterator();

    if(iterator.hasNext())
      vertex = (OrientVertex) iterator.next();

    return vertex;

  }


  /**
   * @param record
   * @throws SQLException 
   */
  public Vertex upsertVisitedVertex(ResultSet record, OVertexType vertexType, ODrakkarContext context) throws SQLException {

    OrientGraphFactory factory = new OrientGraphFactory(this.graphDBUrl);
    OrientGraphNoTx orientGraph = factory.getNoTx();
    orientGraph.setStandardElementConstraints(false);
    Map<String,String> properties = new LinkedHashMap<String,String>();

    // building keys and values for the lookup

    List<String> propertiesOfIndex = new LinkedList<String>();

    for(OModelProperty currentProperty: vertexType.getProperties()) {
      // only attribute coming from the primary key are given
      if(currentProperty.isFromPrimaryKey())
        propertiesOfIndex.add(currentProperty.getName());
    }

    String[] propertyOfKey = new String[propertiesOfIndex.size()];
    String[] valueOfKey = new String[propertiesOfIndex.size()];

    int cont = 0;
    for(String property: propertiesOfIndex) {
      propertyOfKey[cont] = property;
      valueOfKey[cont] = record.getString(context.getNameResolver().reverseTransformation(property));
      cont++;
    }

    String s = "Keys and values in the lookup (upsertVisitedVertex):\t";
    for(int i=0; i<propertyOfKey.length;i++) {
      s += propertyOfKey[i] + ":" + valueOfKey[i];
    }
    context.getOutputManager().debug(s);

    OrientVertex vertex = this.getVertexByIndexedKey(orientGraph, propertyOfKey, valueOfKey, vertexType.getType());  // !!!

    // setting properties to the vertex
    String currentAttributeValue = null;
    String currentDateValue;
    String currentPropertyType;
    for(OModelProperty currentProperty : vertexType.getProperties()) {

      currentPropertyType = context.getDataTypeHandler().resolveType(currentProperty.getPropertyType().toLowerCase(Locale.ENGLISH),context).toString();
      currentAttributeValue = record.getString(context.getNameResolver().reverseTransformation(currentProperty.getName()));

      if(currentAttributeValue != null) {

        if(currentPropertyType.equals("DATE")) {
          currentDateValue = record.getDate(context.getNameResolver().reverseTransformation(currentProperty.getName())).toString();
          properties.put(currentProperty.getName(), currentDateValue);
        }

        else if(currentPropertyType.equals("DATETIME")) {
          {
            currentDateValue = record.getTimestamp(context.getNameResolver().reverseTransformation(currentProperty.getName())).toString();
            properties.put(currentProperty.getName(), currentDateValue);
          }
        }

        else if(currentPropertyType.equals("BOOLEAN")) {
          switch(currentAttributeValue) {

          case "t": properties.put(currentProperty.getName(), "true");
          break;
          case "f": properties.put(currentProperty.getName(), "false");
          break;
          default: break;
          }
        }

        else {
          properties.put(currentProperty.getName(), currentAttributeValue);
        }
      }
      else {
        // null value is inserted in the property
        properties.put(currentProperty.getName(), currentAttributeValue);
      }
    }

    if(vertex == null) {
      String classAndClusterName = vertexType.getType(); 
      vertex = orientGraph.addVertex("class:"+classAndClusterName, properties);
    }
    else {

      // removing old eventual properties
      for(String propertyKey: vertex.getPropertyKeys()) {
        vertex.removeProperty(propertyKey);
      }
      vertex.save();

      // setting new properties
      vertex.setProperties(properties);
      vertex.save();
    }

    context.getOutputManager().debug(properties.toString());
    context.getOutputManager().debug("New vertex inserted (all props setted): " + vertex.toString() + "\n");
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

  public OrientVertex upsertReachedVertexWithEdge(ResultSet foreignRecord, ORelationship relation, OrientVertex currentOutVertex, OVertexType currentInVertexType,
      String edgeType, ODrakkarContext context) throws SQLException {

    OrientGraphFactory factory = new OrientGraphFactory(this.graphDBUrl);
    OrientGraphNoTx orientGraph = factory.getNoTx();
    orientGraph.setStandardElementConstraints(false);

    // building keys and values for the lookup 

    String[] propertyOfKey = new String[relation.getForeignKey().getInvolvedAttributes().size()];
    String[] valueOfKey = new String[relation.getForeignKey().getInvolvedAttributes().size()];

    int index = 0;
    for(OAttribute foreignAttribute: relation.getForeignKey().getInvolvedAttributes())  {
      propertyOfKey[index] = context.getNameResolver().resolveVertexProperty(relation.getPrimaryKey().getInvolvedAttributes().get(index).getName());
      valueOfKey[index] = foreignRecord.getString((foreignAttribute.getName()));
      index++;
    }

    String s = "Keys and values in the lookup (upsertReachedVertex):\t";
    for(int i=0; i<propertyOfKey.length;i++) {
      s += propertyOfKey[i] + ":" + valueOfKey[i] + "\t";
    }
    context.getOutputManager().debug(s);

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

        context.getOutputManager().debug("NEW Reached vertex (id:value) --> " + Arrays.toString(propertyOfKey) + ":" + Arrays.toString(valueOfKey));
        String classAndClusterName = currentInVertexType.getType(); 
        currentInVertex = orientGraph.addVertex("class:"+classAndClusterName, partialProperties);
        context.getOutputManager().debug("New vertex inserted (only pk props setted): " + currentInVertex.toString() + "\n");

      }

      else {
        context.getOutputManager().debug("NOT NEW Reached vertex, vertex " + Arrays.toString(propertyOfKey) + ":" + Arrays.toString(valueOfKey) + " already present in the Orient Graph.\n");
      }

      // upsert of the edge between the currentOutVertex and the currentInVertex
      this.upsertEdge(orientGraph, currentOutVertex, currentInVertex, edgeType, context);
    }
    orientGraph.shutdown();

    return currentInVertex;
  }

  public void upsertEdge(OrientGraphNoTx orientGraph, OrientVertex currentOutVertex, OrientVertex currentInVertex, String edgeType, ODrakkarContext context) {

    boolean edgeAlreadyPresent = false;
    Iterator<Edge> it = currentOutVertex.getEdges(Direction.OUT, edgeType).iterator();
    Edge currentEdge;

    if(it.hasNext()) {

      while(it.hasNext()) {
        currentEdge = it.next();

        if(((OrientVertex)currentEdge.getVertex(Direction.IN)).getId().equals(currentInVertex.getId())) {
          edgeAlreadyPresent = true;
          break;
        }

      }

      if(edgeAlreadyPresent) {
        context.getOutputManager().debug("Edge beetween '" + currentOutVertex.toString() + "' and '" + currentInVertex.toString() + "' already present.");
      }
      else {
        OrientEdge edge = orientGraph.addEdge(null, currentOutVertex, currentInVertex, edgeType);
        edge.save();
        context.getOutputManager().debug("New edge inserted: " + edge.toString());
      }

    }
    else {
      OrientEdge edge = orientGraph.addEdge(null, currentOutVertex, currentInVertex, edgeType);
      edge.save();
      context.getOutputManager().debug("New edge inserted: " + edge.toString());
    }
  }

  public void upsertAggregatorEdge(ResultSet jointTableRecord, OEntity joinTable, OAggregatorEdge aggregatorEdge, ODrakkarContext context) throws SQLException {

    OrientGraphFactory factory = new OrientGraphFactory(this.graphDBUrl);
    OrientGraphNoTx orientGraph = factory.getNoTx();
    orientGraph.setStandardElementConstraints(false);

    ORelationship relationship1 = joinTable.getRelationships().get(0);
    ORelationship relationship2 = joinTable.getRelationships().get(1);


    // Building keys and values for out-vertex lookup

    String[] keysOutVertex = new String[relationship1.getForeignKey().getInvolvedAttributes().size()];
    String[] valuesOutVertex = new String[relationship1.getForeignKey().getInvolvedAttributes().size()];

    int index = 0;
    for(OAttribute foreignKeyAttribute: relationship1.getForeignKey().getInvolvedAttributes()) {
      keysOutVertex[index] = context.getNameResolver().resolveVertexProperty(relationship1.getPrimaryKey().getInvolvedAttributes().get(index).getName());
      valuesOutVertex[index] = jointTableRecord.getString(foreignKeyAttribute.getName());
      index++;
    }


    // Building keys and values for in-vertex lookup

    String[] keysInVertex = new String[relationship2.getPrimaryKey().getInvolvedAttributes().size()];
    String[] valuesInVertex = new String[relationship2.getPrimaryKey().getInvolvedAttributes().size()];

    index = 0;
    for(OAttribute foreignKeyAttribute: relationship2.getForeignKey().getInvolvedAttributes()) {
      keysInVertex[index] = context.getNameResolver().resolveVertexProperty(relationship2.getPrimaryKey().getInvolvedAttributes().get(index).getName());
      valuesInVertex[index] = jointTableRecord.getString(foreignKeyAttribute.getName());
      index++;
    }

    OrientVertex currentOutVertex = this.getVertexByIndexedKey(orientGraph, keysOutVertex, valuesOutVertex, aggregatorEdge.getOutVertexClassName());
    OrientVertex currentInVertex = this.getVertexByIndexedKey(orientGraph, keysInVertex, valuesInVertex, aggregatorEdge.getInVertexClassName());

    this.upsertEdge(orientGraph, currentOutVertex, currentInVertex, aggregatorEdge.getEdgeType(), context);
    orientGraph.shutdown();
  }

}
