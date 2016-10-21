/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * OrientDB LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * OrientDB LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from OrientDB LTD.
 * 
 * For more information: http://www.orientdb.com
 */

package com.orientechnologies.teleporter.importengine.rdbms.graphengine;

import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.mapper.rdbms.OAggregatorEdge;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OAttribute;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.ORelationship;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OModelProperty;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.util.OFunctionsHandler;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Executes the necessary operations of insert and upsert for the destination Orient DB populating.
 *
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public class OGraphEngineForDB {

  private OER2GraphMapper      mapper;
  private ODBMSDataTypeHandler handler;

  public OGraphEngineForDB(OER2GraphMapper mapper, ODBMSDataTypeHandler handler) {
    this.mapper = mapper;
    this.handler = handler;
  }


  /**
   * Return true if the record is "full-imported" in OrientDB: the correspondent vertex is visited (all properties are set).
   * @param record
   * @throws SQLException
   */
  public boolean alreadyFullImportedInOrient(OrientBaseGraph orientGraph, ResultSet record, OVertexType vertexType, Set<String> propertiesOfIndex) throws SQLException {

    String propsAndValuesOfKey = "";

    try {

      boolean toResolveNames = false;
      // building keys and values for the lookup

      if(propertiesOfIndex == null) {
        toResolveNames = true;
        propertiesOfIndex = new LinkedHashSet<String>();

        for(OModelProperty currentProperty: vertexType.getAllProperties()) {
          // only attribute coming from the primary key are given
          if(currentProperty.isFromPrimaryKey())
            propertiesOfIndex.add(currentProperty.getName());
        }
      }

      String[] propertyOfKey = new String[propertiesOfIndex.size()];
      String[] valueOfKey = new String[propertiesOfIndex.size()];

      int cont = 0;
      for(String property: propertiesOfIndex) {
        propertyOfKey[cont] = property;
        if(toResolveNames)
          valueOfKey[cont] = record.getString(this.mapper.getAttributeNameByVertexTypeAndProperty(vertexType, property));
        else
          valueOfKey[cont] = record.getString(property);

        cont++;
      }

      String s = "Keys and values in the lookup (upsertVisitedVertex):\t";
      for(int i=0; i<propertyOfKey.length;i++) {
        propsAndValuesOfKey += propertyOfKey[i] + ":" + valueOfKey[i] + ",";
      }
      if(propsAndValuesOfKey.length() > 0)
        propsAndValuesOfKey = propsAndValuesOfKey.substring(0, propsAndValuesOfKey.length()-1);
      else
        propsAndValuesOfKey = "no identifier for the current record.";
      s += propsAndValuesOfKey;

      // lookup
      OrientVertex vertex = this.getVertexByIndexedKey(orientGraph, propertyOfKey, valueOfKey, vertexType.getName());

      if(vertex != null && vertexType.getAllProperties().size() <= vertex.getPropertyKeys().size()) // there aren't properties to add into the vertex (<=)
        return true;

    } catch (Exception e) {
      String mess =  "Problem encountered during the visit of an inserted vertex. Vertex Type: " + vertexType.getName() + ";\tOriginal Record: " + propsAndValuesOfKey;
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      if(orientGraph != null)
        orientGraph.shutdown();
      throw new OTeleporterRuntimeException(e);
    }
    return false;
  }



  /**
   * The method perform on the passed OrientBaseGraph a lookup for a OrientVertex starting from a record and from a vertex type.
   * It return the vertex if present, null if not present. 
   *
   * @param orientGraph
   * @param keys
   * @param values
   * @param vertexClassName
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
   */
  public Vertex upsertVisitedVertex(OrientBaseGraph orientGraph, ResultSet record, OVertexType vertexType, Set<String> propertiesOfIndex) {

    OrientVertex vertex = null;
    String[] propertyOfKey = null;
    String[] valueOfKey = null;
    String propsAndValuesOfKey = "";

    try {

      OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();

      boolean toResolveNames = false;

      // building keys and values for the lookup

      if(propertiesOfIndex == null) {
        toResolveNames = true;
        propertiesOfIndex = new LinkedHashSet<String>();

        for(OModelProperty currentProperty: vertexType.getAllProperties()) {
          // only attribute coming from the primary key are given
          if(currentProperty.isFromPrimaryKey()) {
            propertiesOfIndex.add(currentProperty.getName());
          }
        }
      }

      propertyOfKey = new String[propertiesOfIndex.size()];
      valueOfKey = new String[propertiesOfIndex.size()];
      String currentValue;

      int cont = 0;
      for(String property: propertiesOfIndex) {
        propertyOfKey[cont] = property;
        if(toResolveNames) {
          String attributeName = this.mapper.getAttributeNameByVertexTypeAndProperty(vertexType, property);
          currentValue = record.getString(attributeName);
        }
        else {
          currentValue = record.getString(property);
        }

        // converting eventual "t" or "f" values in "true" and "false"
        OModelProperty prop = vertexType.getPropertyByNameAmongAll(property);
        if(prop.getOriginalType().equalsIgnoreCase("boolean")) {
          switch(currentValue) {
            case "t": currentValue = "true";
              break;
            case "f": currentValue = "false";
              break;
            default: break;
          }
        }

        valueOfKey[cont] = currentValue;
        cont++;
      }

      String s = "Keys and values in the lookup (upsertVisitedVertex):\t";
      for(int i=0; i<propertyOfKey.length;i++) {
        propsAndValuesOfKey += propertyOfKey[i] + ":" + valueOfKey[i] + ",";
      }
      if(propsAndValuesOfKey.length() > 0)
        propsAndValuesOfKey = propsAndValuesOfKey.substring(0, propsAndValuesOfKey.length()-1);
      else
        propsAndValuesOfKey = "no identifier for the current record.";
      s += propsAndValuesOfKey;
      OTeleporterContext.getInstance().getOutputManager().debug("\n" + s + "\n");

      // lookup (only if properties and values are different from null)
      if(propertyOfKey.length > 0 && valueOfKey.length > 0 )
        vertex = this.getVertexByIndexedKey(orientGraph, propertyOfKey, valueOfKey, vertexType.getName());

      // extraction of inherited and not inherited properties from the record (through "getAllProperties()" method)
      Map<String,Object> properties = new LinkedHashMap<String,Object>();
      String currentPropertyType;
      String currentPropertyName = null;

      for(OModelProperty currentProperty : vertexType.getAllProperties()) {

        if (currentProperty.isIncludedInMigration()) {
          currentPropertyName = currentProperty.getName();
          currentPropertyType = OTeleporterContext.getInstance().getDataTypeHandler().resolveType(currentProperty.getOriginalType().toLowerCase(Locale.ENGLISH)).toString();
          String currentOriginalType = currentProperty.getOriginalType();

          try {
            extractPropertiesFromRecord(record, properties, currentPropertyType, currentPropertyName, currentOriginalType, vertexType);
          } catch (Exception e) {
            String mess = "Problem encountered during the extraction of the values from the records. Vertex Type: " + vertexType.getName() + ";\tProperty: " + currentPropertyName + ";\tRecord: " + propsAndValuesOfKey;
            OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
            OTeleporterContext.getInstance().printExceptionStackTrace(e, "debug");
          }
        }
      }
      if(vertexType.getName().equalsIgnoreCase("generationnotificationevent")) {
        int a = 4+5;
      }

      if(vertex == null) {
        String classAndClusterName = vertexType.getName();
        vertex = this.addVertexToGraph(orientGraph, classAndClusterName, properties);
        statistics.orientAddedVertices++;
        OTeleporterContext.getInstance().getOutputManager().debug("\nLoaded properties: %s\n", properties.toString());
        OTeleporterContext.getInstance().getOutputManager().debug("\nNew vertex inserted (all props setted): %s\n", vertex.toString());
      }
      else {

        // discerning between a reached-vertex updating (only original primary key's properties are present) and a full-vertex updating
        boolean justReachedVertex = true;

        // first check: if the current vertex has less properties then the correspondent vertex type then it's justReached
        if(vertex.getPropertyKeys().size() >= vertexType.getAllProperties().size()) {
          justReachedVertex = false;
        }

        // second check: the set properties on the current vertex are only those correspondent to the primary key's attributes
        if(justReachedVertex) {
          for(String property: vertex.getPropertyKeys()) {
            if(!this.containsProperty(propertiesOfIndex,property)) {
              justReachedVertex = false;
              break;
            }
          }
        }

        // UPDATING A REACHED VERTEX (only original primary key's properties are present)
        if(justReachedVertex) {

          // setting new properties and save
          this.setElementProperties(vertex, properties);
          vertex.save();
          OTeleporterContext.getInstance().getOutputManager().debug("\nLoaded properties: %s\n", properties.toString());
          OTeleporterContext.getInstance().getOutputManager().debug("\nNew vertex inserted (all props setted): %s\n", vertex.toString());
        }

        // UPDATING A FULL VERTEX
        else {

          // comparing old version of vertex with the new one: if the two versions are equals no rewriting is performed

          boolean equalVersions = true;
          boolean equalProperties = true;

          if(vertex.getPropertyKeys().size() == properties.size()) {

            // comparing properties
            for(String propertyName: vertex.getPropertyKeys()) {
              if(!properties.keySet().contains(propertyName)) {
                equalProperties = false;
                equalVersions = false;
                break;
              }
            }

            if(equalProperties) {
              // comparing values of the properties
              for(String propertyName: vertex.getPropertyKeys()) {
                if(!(vertex.getProperty(propertyName) == null && properties.get(propertyName) == null) ) {
                  currentPropertyType = OTeleporterContext.getInstance().getDataTypeHandler().resolveType(vertexType.getPropertyByName(propertyName).getOriginalType().toLowerCase(Locale.ENGLISH)).toString();
                  if(!this.areEquals(vertex.getProperty(propertyName), properties.get(propertyName), currentPropertyType, currentPropertyName)) {
                    equalVersions = false;
                    break;
                  }
                }
              }
            }
            else {
              equalVersions = false;
            }
          }
          else {
            equalVersions = false;
          }

          if(!equalVersions) {
            // removing old eventual properties
            for(String propertyKey: vertex.getPropertyKeys()) {
              vertex.removeProperty(propertyKey);
            }

            // setting new properties and save
            this.setElementProperties(vertex, properties);
            statistics.orientUpdatedVertices++;
            OTeleporterContext.getInstance().getOutputManager().debug("\nLoaded properties: %s\n", properties.toString());
            OTeleporterContext.getInstance().getOutputManager().debug("\nNew vertex upserted (all props setted): %s\n", vertex.toString());
          }
        }
      }
    } catch (Exception e) {
      String mess =  "Problem encountered during the migration of the records. Vertex Type: " + vertexType.getName() + ";\tRecord: " + propsAndValuesOfKey;
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      if(orientGraph != null)
        orientGraph.shutdown();
      throw new OTeleporterRuntimeException(e);
    }

    return vertex;
  }

  private void extractPropertiesFromRecord(ResultSet record, Map<String, Object> properties, String currentPropertyType, String currentPropertyName, String currentOriginalType, OVertexType vertexType) throws SQLException {

    Date currentDateValue;
    byte[] currentBinaryValue;
    String currentAttributeValue;

    // disambiguation on OrientDB Schema type

    if(currentPropertyType.equals("DATE")) {
      currentDateValue = record.getDate(this.mapper.getAttributeNameByVertexTypeAndProperty(vertexType, currentPropertyName));
      properties.put(currentPropertyName, currentDateValue);
    }

    else if(currentPropertyType.equals("DATETIME")) {
      currentDateValue = record.getTimestamp(this.mapper.getAttributeNameByVertexTypeAndProperty(vertexType, currentPropertyName));
      properties.put(currentPropertyName, currentDateValue);
    }

    else if(currentPropertyType.equals("BINARY")) {
      currentBinaryValue = record.getBytes(this.mapper.getAttributeNameByVertexTypeAndProperty(vertexType, currentPropertyName));
      properties.put(currentPropertyName, currentBinaryValue);
    }

    else if(currentPropertyType.equals("BOOLEAN")) {
      currentAttributeValue = record.getString(this.mapper.getAttributeNameByVertexTypeAndProperty(vertexType, currentPropertyName));

      switch(currentAttributeValue) {

        case "t": properties.put(currentPropertyName, "true");
          break;
        case "f": properties.put(currentPropertyName, "false");
          break;
        default: break;
      }
    }

    // JSON
    else if(handler.jsonImplemented && currentPropertyType.equals("EMBEDDED")) {
      currentAttributeValue = record.getString(this.mapper.getAttributeNameByVertexTypeAndProperty(vertexType, currentPropertyName));
      ODocument currentEmbeddedValue = this.handler.convertJSONToDocument(currentPropertyName, currentAttributeValue);
      properties.put(currentPropertyName, currentEmbeddedValue);
    }

    // GEOSPATIAL
    else if(handler.geospatialImplemented && handler.isGeospatial(currentOriginalType)) {
      currentAttributeValue = record.getString(this.mapper.getAttributeNameByVertexTypeAndProperty(vertexType, currentPropertyName));
      //						currentEmbeddedValue = OShapeFactory.INSTANCE.toDoc(currentAttributeValue);  // to change with transformation from wkt (currentAttrValue) into embedded
      ODocument currentEmbeddedValue = null;
      properties.put(currentPropertyName, currentEmbeddedValue);
    }

    else {
      currentAttributeValue = record.getString(this.mapper.getAttributeNameByVertexTypeAndProperty(vertexType, currentPropertyName));
      properties.put(currentPropertyName, currentAttributeValue);
    }

  }


  /**
   * @param propertiesOfIndex
   * @param property
   * @return
   */
  private boolean containsProperty(Set<String> propertiesOfIndex, String property) {

    for(String currentProp: propertiesOfIndex) {
      if(currentProp.equalsIgnoreCase(property))
        return true;
    }

    return false;
  }


  /**
   * @param newProperty
   * @param currentPropertyType
   * @param currentPropertyName
   * @return
   */
  private boolean areEquals(Object oldProperty, Object newProperty, String currentPropertyType, String currentPropertyName) {

    if(oldProperty != null && newProperty != null) {

      if(currentPropertyType.equals("BINARY")) {
        byte[] oldPropertyBinary = (byte[]) oldProperty;
        byte[] newPropertyBinary = (byte[]) newProperty;
        return Arrays.equals(oldPropertyBinary, newPropertyBinary);
      }

      else if(currentPropertyType.equals("BOOLEAN")) {

        if (oldProperty.toString().equalsIgnoreCase(newProperty.toString()))
          return true;

        else if(oldProperty.toString().equalsIgnoreCase("t") && newProperty.toString().equalsIgnoreCase("true")
                || oldProperty.toString().equalsIgnoreCase("f") && newProperty.toString().equalsIgnoreCase("false"))
          return true;

        else
          return false;
      }

      else if(currentPropertyType.equals("DATE")) {

        // oldProperty : Date (year, month, day)
        Calendar oldDate = Calendar.getInstance();
        oldDate.setTime((Date)oldProperty);
        Calendar newDate = Calendar.getInstance();
        newDate.setTime((Date)newProperty);

        if(oldDate.get(Calendar.ERA) == newDate.get(Calendar.ERA) &&
                oldDate.get(Calendar.YEAR) == newDate.get(Calendar.YEAR) &&
                oldDate.get(Calendar.MONTH) == newDate.get(Calendar.MONTH) &&
                oldDate.get(Calendar.DAY_OF_MONTH) == newDate.get(Calendar.DAY_OF_MONTH)) {
          return true;
        }
        else {
          return false;
        }
      }

      else if(currentPropertyType.equals("DATETIME")) {
        // oldProperty : Date (year, month, day, hours, minutes, seconds, millis)
        return ((Date)oldProperty).equals((Date)newProperty);
      }

      else if(currentPropertyType.equals("DECIMAL")) {
        return oldProperty.equals(new BigDecimal(newProperty.toString()));
      }

      else if(currentPropertyType.equals("DOUBLE")) {
        return oldProperty.equals(new Double(newProperty.toString()));
      }

      else if(currentPropertyType.equals("FLOAT")) {
        return oldProperty.equals(new Float(newProperty.toString()));
      }

      else if(currentPropertyType.equals("INTEGER")) {
        return oldProperty.equals(new Integer(newProperty.toString()));
      }

      else if(currentPropertyType.equals("LONG")) {
        return oldProperty.equals(new Long(newProperty.toString()));
      }

      else if(currentPropertyType.equals("SHORT")) {
        return oldProperty.equals(new Short(newProperty.toString()));
      }

      else if(handler.jsonImplemented && currentPropertyType.equals("EMBEDDED")) {
        boolean areEquals = OFunctionsHandler.haveDocumentsSameContent(((ODocument) oldProperty), ((ODocument) newProperty));
        return areEquals;
      }

      else {
        return oldProperty.toString().equals(newProperty.toString());
      }
    }

    else if(oldProperty == null && newProperty == null)
      return true;

    else
      return false;
  }



  /**
   * @param foreignRecord the record correspondent to the current-out-vertex
   * @param relation the relation between two entities
   * @param currentOutVertex the current-out-vertex
   * @param currentInVertexType the type correspondent to the current-in-vertex
   * @param edgeType type of the OEdgeType present between the two OVertexType, used as label during the insert of the edge in the graph
   *
   * The method executes insert on reached vertex:
   * - if the vertex is not already reached, it's inserted in the graph and an edge between the out-visited-vertex and the in-reached-vertex is added
   * - if the vertex is already present in the graph no update is performed, neither on reached-vertex neither on the relative edge
   * @throws SQLException
   */

  public OrientVertex upsertReachedVertexWithEdge(OrientBaseGraph orientGraph, ResultSet foreignRecord, ORelationship relation, OrientVertex currentOutVertex, OVertexType currentInVertexType,
                                                  String edgeType) throws SQLException {

    OrientVertex currentInVertex = null;
    String propsAndValuesOfKey = "";
    String direction = relation.getDirection();

    try {

      OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();

      // building keys and values for the lookup 

      String[] propertyOfKey = new String[relation.getForeignKey().getInvolvedAttributes().size()];
      String[] valueOfKey = new String[relation.getForeignKey().getInvolvedAttributes().size()];

      int index = 0;
      for(OAttribute foreignAttribute: relation.getForeignKey().getInvolvedAttributes())  {
        String attributeName = relation.getPrimaryKey().getInvolvedAttributes().get(index).getName();
        propertyOfKey[index] =  mapper.getPropertyNameByVertexTypeAndAttribute(currentInVertexType, attributeName);
        valueOfKey[index] = foreignRecord.getString((foreignAttribute.getName()));
        index++;
      }

      String s = "Keys and values in the lookup (upsertVisitedVertex):\t";
      for(int i=0; i<propertyOfKey.length;i++) {
        propsAndValuesOfKey += propertyOfKey[i] + ":" + valueOfKey[i] + ",";
      }
      if(propsAndValuesOfKey.length() > 0)
        propsAndValuesOfKey = propsAndValuesOfKey.substring(0, propsAndValuesOfKey.length()-1);
      else
        propsAndValuesOfKey = "no identifier for the current record.";
      s += propsAndValuesOfKey;
      OTeleporterContext.getInstance().getOutputManager().debug("\n" + s + "\n");

      // new vertex is added only if all the values in the foreign key are different from null
      boolean ok = true;

      for(int i=0; i<valueOfKey.length; i++) {
        if(valueOfKey[i] == null) {
          ok = false;
          break;
        }
      }

      // all values are different from null, thus vertex is searched in the graph and in case is added if not found.
      if(ok) {

        currentInVertex = this.getVertexByIndexedKey(orientGraph, propertyOfKey, valueOfKey, currentInVertexType.getName());

        /*
         *  if the vertex is not already present in the graph it's built, set and inserted to the graph,
         *  then the edge beetwen the current-out-vertex and the current-in-vertex is added 
         */
        if(currentInVertex == null) {

          Map<String,Object> partialProperties = new LinkedHashMap<String,Object>();

          // for each attribute in the foreign key belonging to the relationship, attribute name and correspondent value are added to a 'properties map'
          for(int i=0; i<propertyOfKey.length; i++) {
            partialProperties.put(propertyOfKey[i], valueOfKey[i]);
          }

          OTeleporterContext.getInstance().getOutputManager()
                  .debug("\nNEW Reached vertex (id:value) --> %s:%s\n", Arrays.toString(propertyOfKey), Arrays.toString(valueOfKey));
          String classAndClusterName = currentInVertexType.getName();
          currentInVertex = this.addVertexToGraph(orientGraph, classAndClusterName, partialProperties);
          statistics.orientAddedVertices++;
          OTeleporterContext.getInstance().getOutputManager().debug("\nNew vertex inserted (only pk props setted): %s\n", currentInVertex.toString());

        }

        else {
          OTeleporterContext.getInstance().getOutputManager()
                  .debug("\nNOT NEW Reached vertex, vertex %s:%s already present in the Orient Graph.\n", Arrays.toString(propertyOfKey), Arrays.toString(valueOfKey));
        }

        // upsert of the edge between the currentOutVertex and the currentInVertex
        this.upsertEdge(orientGraph, currentOutVertex, currentInVertex, edgeType, null, direction);
      }

    } catch (Exception e) {
      String mess =  "Problem encountered during the upsert of a reached vertex. Vertex Type: " + currentInVertexType.getName() + ";\tOriginal Record: " + propsAndValuesOfKey;
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      if(orientGraph != null)
        orientGraph.shutdown();
      throw new OTeleporterRuntimeException(e);
    }

    return currentInVertex;
  }

  public void upsertEdge(OrientBaseGraph orientGraph, OrientVertex currentOutVertex, OrientVertex currentInVertex, String edgeType, Map<String, Object> properties, String direction) {

    try {

      boolean edgeAlreadyPresent = false;
      Iterator<Edge> it = currentOutVertex.getEdges(Direction.OUT, edgeType).iterator();
      Edge currentEdge;

      OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();

      if(it.hasNext()) {
        while(it.hasNext()) {
          currentEdge = it.next();

          if(((OrientVertex)currentEdge.getVertex(Direction.IN)).getId().equals(currentInVertex.getId())) {
            edgeAlreadyPresent = true;
            break;
          }
        }
        if(edgeAlreadyPresent) {
          OTeleporterContext.getInstance().getOutputManager().debug("\nEdge beetween '%s' and '%s' already present.\n", currentOutVertex.toString(), currentInVertex.toString());
        }
        else {
          OrientEdge edge = null;
          if(direction != null && direction.equals("direct")) {
            edge = this.addEdgeToGraph(orientGraph, null, currentOutVertex, currentInVertex, edgeType);
          }
          else if(direction != null && direction.equals("inverse")) {
            edge = this.addEdgeToGraph(orientGraph, null, currentInVertex, currentOutVertex, edgeType);
          }
          this.setElementProperties(edge, properties);
          statistics.orientAddedEdges++;
          OTeleporterContext.getInstance().getOutputManager().debug("\nNew edge inserted: %s\n", edge.toString());
        }
      }
      else {
        OrientEdge edge = null;
        if(direction != null && direction.equals("direct")) {
          edge = this.addEdgeToGraph(orientGraph, null, currentOutVertex, currentInVertex, edgeType);
        }
        else if(direction != null && direction.equals("inverse")) {
          edge = this.addEdgeToGraph(orientGraph, null, currentInVertex, currentOutVertex, edgeType);
        }
        this.setElementProperties(edge, properties);
        statistics.orientAddedEdges++;
        OTeleporterContext.getInstance().getOutputManager().debug("\nNew edge inserted: %s\n", edge.toString());
      }
    } catch (Exception e) {
      String mess =  "Problem encountered during the upsert of an edge. Vertex-out: " + currentOutVertex + ";\tVertex-in: " + currentInVertex;
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      if(orientGraph != null)
        orientGraph.shutdown();
      throw new OTeleporterRuntimeException(e);
    }
  }

  public void upsertAggregatorEdge(OrientBaseGraph orientGraph, ResultSet jointTableRecord, OEntity joinTable, OAggregatorEdge aggregatorEdge) throws SQLException {

    try {

      Iterator<ORelationship> it = joinTable.getOutRelationships().iterator();
      ORelationship relationship1 = it.next();
      ORelationship relationship2 = it.next();


      // Building keys and values for out-vertex lookup

      String[] keysOutVertex = new String[relationship1.getForeignKey().getInvolvedAttributes().size()];
      String[] valuesOutVertex = new String[relationship1.getForeignKey().getInvolvedAttributes().size()];

      int index = 0;
      for(OAttribute foreignKeyAttribute: relationship1.getForeignKey().getInvolvedAttributes()) {
        keysOutVertex[index] = this.mapper.getPropertyNameByEntityAndAttribute(relationship1.getParentEntity(), relationship1.getPrimaryKey().getInvolvedAttributes().get(index).getName());
        valuesOutVertex[index] = jointTableRecord.getString(foreignKeyAttribute.getName());
        index++;
      }

      // Building keys and values for in-vertex lookup

      String[] keysInVertex = new String[relationship2.getPrimaryKey().getInvolvedAttributes().size()];
      String[] valuesInVertex = new String[relationship2.getPrimaryKey().getInvolvedAttributes().size()];

      index = 0;
      for(OAttribute foreignKeyAttribute: relationship2.getForeignKey().getInvolvedAttributes()) {
        keysInVertex[index] = this.mapper.getPropertyNameByEntityAndAttribute(relationship2.getParentEntity(), relationship2.getPrimaryKey().getInvolvedAttributes().get(index).getName());
        valuesInVertex[index] = jointTableRecord.getString(foreignKeyAttribute.getName());
        index++;
      }

      // String direction
      String direction = joinTable.getDirectionOfN2NRepresentedRelationship();


      OrientVertex currentOutVertex;
      OrientVertex currentInVertex;
      if(direction.equals("direct")) {
        currentOutVertex = this.getVertexByIndexedKey(orientGraph, keysOutVertex, valuesOutVertex, aggregatorEdge.getOutVertexClassName());
        currentInVertex = this.getVertexByIndexedKey(orientGraph, keysInVertex, valuesInVertex, aggregatorEdge.getInVertexClassName());
      }
      else {
        currentOutVertex = this.getVertexByIndexedKey(orientGraph, keysOutVertex, valuesOutVertex, aggregatorEdge.getInVertexClassName());
        currentInVertex = this.getVertexByIndexedKey(orientGraph, keysInVertex, valuesInVertex, aggregatorEdge.getOutVertexClassName());
      }

      // extracting edge properties from the join table
      Map<String,Object> properties = new LinkedHashMap<String,Object>();
      OEdgeType edgeType = aggregatorEdge.getEdgeType();

      for(OModelProperty currentProperty: edgeType.getAllProperties()) {

        String currentPropertyName = currentProperty.getName();
        String currentPropertyType = currentProperty.getOrientdbType();
        if(currentProperty.getOrientdbType() == null) { // superfluous ?!
          currentPropertyType = OTeleporterContext.getInstance().getDataTypeHandler().resolveType(currentProperty.getOriginalType().toLowerCase(Locale.ENGLISH)).toString();
        }
        String currentOriginalType = currentProperty.getOriginalType();
        OVertexType joinVertexType = this.mapper.getJoinVertexTypeByAggregatorEdge(edgeType.getName());

        try {
          extractPropertiesFromRecord(jointTableRecord, properties, currentPropertyType, currentPropertyName, currentOriginalType, joinVertexType);
        } catch (Exception e) {
          String mess =  "Problem encountered during the extraction of the values from the records. Edge Type: " + edgeType.getName() + ";\tProperty: " + currentProperty.getName() + ";\tOriginal join table: " + joinTable.getName();
          OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
          OTeleporterContext.getInstance().printExceptionStackTrace(e, "debug");
        }
      }

      this.upsertEdge(orientGraph, currentOutVertex, currentInVertex, aggregatorEdge.getEdgeType().getName(), properties, direction);

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      if(orientGraph != null )
        orientGraph.shutdown();
      throw new OTeleporterRuntimeException(e);
    }
  }


  private OrientVertex addVertexToGraph(OrientBaseGraph orientGraph, String classAndClusterName, Map<String,Object> properties) {

    try {
      if(classAndClusterName != null)
        return orientGraph.addVertex("class:" + classAndClusterName, properties);
    } catch (OValidationException e) {
      OTeleporterContext.getInstance().getStatistics().errorMessages.add(e.getMessage());
    }
    return null;
  }

  private OrientEdge addEdgeToGraph(OrientBaseGraph orientGraph, Object id, OrientVertex currentOutVertex, OrientVertex currentInVertex, String edgeType) {

    try {
      return orientGraph.addEdge(id, currentOutVertex, currentInVertex, edgeType);
    } catch (OValidationException e) {
      OTeleporterContext.getInstance().getStatistics().errorMessages.add(e.getMessage());
    }
    return null;
  }


  private void setElementProperties(OrientElement element, Map<String,Object> properties) {

    try {
      element.setProperties(properties);
    } catch(OValidationException e) {
      OTeleporterContext.getInstance().getStatistics().errorMessages.add(e.getMessage());
    }

  }

}
