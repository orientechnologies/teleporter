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

package com.orientechnologies.orient.importer.mapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.importer.model.dbschema.OAttribute;
import com.orientechnologies.orient.importer.model.dbschema.ODataBaseSchema;
import com.orientechnologies.orient.importer.model.dbschema.OEntity;
import com.orientechnologies.orient.importer.model.dbschema.OForeignKey;
import com.orientechnologies.orient.importer.model.dbschema.OPrimaryKey;
import com.orientechnologies.orient.importer.model.dbschema.ORelationship;
import com.orientechnologies.orient.importer.model.graphmodel.OAttributeProperties;
import com.orientechnologies.orient.importer.model.graphmodel.OEdgeType;
import com.orientechnologies.orient.importer.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.importer.model.graphmodel.OVertexType;
import com.orientechnologies.orient.importer.nameresolver.ONameResolver;
import com.orientechnologies.orient.importer.persistence.util.ODataSource;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 *
 */

public class OER2GraphMapper implements OSource2GraphMapper {

  private ODataSource dataSource;

  // models
  private ODataBaseSchema dataBaseSchema;
  private OGraphModel graphModel;

  // Rules
  private Map<OEntity,OVertexType> entity2vertexType;  
  private Map<ORelationship,OEdgeType> relationship2edgeType;  
  private Map<String,Integer> edgeTypeName2count;
  
  public OER2GraphMapper (String driver, String uri, String username, String password) {
    this.dataSource = new ODataSource(driver, uri, username, password);
    this.entity2vertexType = new HashMap<OEntity,OVertexType>();
    this.relationship2edgeType = new HashMap<ORelationship,OEdgeType>();
    this.edgeTypeName2count = new TreeMap<String,Integer>();
  }


  public void buildSourceSchema() {

    Connection connection = null;

    try {

      connection = this.dataSource.getConnection();
      DatabaseMetaData databaseMetaData = connection.getMetaData();

      /*
       *  General DB Info
       */

      int majorVersion = databaseMetaData.getDatabaseMajorVersion();
      int minorVersion = databaseMetaData.getDatabaseMinorVersion();
      int driverMajorVersion = databaseMetaData.getDriverMajorVersion();
      int driverMinorVersion = databaseMetaData.getDriverMinorVersion();
      String productName = databaseMetaData.getDatabaseProductName();
      String productVersion = databaseMetaData.getDatabaseProductVersion();

      this.dataBaseSchema = new ODataBaseSchema(majorVersion, minorVersion, driverMajorVersion, driverMinorVersion, productName, productVersion);

      List<String> tablesName = new ArrayList<String>();

      String tableCatalog = null;
      String tableSchemaPattern = null;
      String tableNamePattern = null;
      String[] tableTypes = {"TABLE"};

      ResultSet resultTable = databaseMetaData.getTables(tableCatalog, tableSchemaPattern, tableNamePattern, tableTypes);
      ResultSet resultColumns;
      ResultSet resultPrimaryKeys;
      ResultSet resultForeignKeys;

      // Giving db's table names
      while(resultTable.next()) {
        String tableName = resultTable.getString(3);
        tablesName.add(tableName);  
      }

      int numberOfTables = tablesName.size();

      OLogManager.instance().info(this, "%s tables found.", numberOfTables);

      OEntity currentEntity;
      OPrimaryKey pKey;

      List<LinkedHashMap<String,String>> currentEntityRelationships1;
      List<LinkedHashMap<String,String>> currentEntityRelationships2;

      /*
       *  Entity building
       */
      int iteration = 1;
      for(String currentTableName: tablesName) {

        OLogManager.instance().info(this, "Building '%s' entity (%s/%s)...", currentTableName, iteration, numberOfTables);

        // creating entity
        currentEntity = new OEntity(currentTableName);

        // adding attributes and primary keys
        pKey = new OPrimaryKey(currentEntity);

        String columnCatalog = null;
        String columnSchemaPattern = null;
        String columnNamePattern = null;

        String primaryKeyCatalog = null;
        String primaryKeySchema = null;

        resultColumns = databaseMetaData.getColumns(columnCatalog, columnSchemaPattern, currentTableName, columnNamePattern);
        resultPrimaryKeys = databaseMetaData.getPrimaryKeys(primaryKeyCatalog, primaryKeySchema, currentTableName);

        while(resultColumns.next()) {
          OAttribute currentAttribute = new OAttribute(resultColumns.getString(4), resultColumns.getInt(17), resultColumns.getString(6), currentEntity);
          currentEntity.addAttribute(currentAttribute);

          // if the current attribute is involved in the primary key, it will be added to the attributes of pKey.
          if(this.isPresentInResultPrimaryKeys(resultPrimaryKeys, currentAttribute.getName())) {
            pKey.addAttribute(currentAttribute);
          }

        }

        currentEntity.setPrimaryKey(pKey);

        // adding entity to db schema
        this.dataBaseSchema.addEntity(currentEntity);

        iteration++;
        OLogManager.instance().info(this, "Entity %s built.\n", currentTableName);
      }


      /*
       *  Building relationships
       */
      iteration = 1;
      for(OEntity currentForeignEntity: this.dataBaseSchema.getEntities()) {

        String currentForeignEntityName = currentForeignEntity.getName();
        OLogManager.instance().info(this, "Building relationships starting from '%s' entity (%s/%s)...", currentForeignEntityName, iteration, numberOfTables);

        String foreignCatalog = null;
        String foreignSchema = null;

        resultForeignKeys = databaseMetaData.getImportedKeys(foreignCatalog, foreignSchema, currentForeignEntityName);

        // copy of resultset in a HashLinkedMap
        currentEntityRelationships1 = this.fromResultSetToList(resultForeignKeys);
        currentEntityRelationships2 = new LinkedList<LinkedHashMap<String,String>>();
        currentEntityRelationships2.addAll(currentEntityRelationships1);

        Iterator<LinkedHashMap<String,String>> it1 = currentEntityRelationships1.iterator();
        Iterator<LinkedHashMap<String,String>> it2 = currentEntityRelationships2.iterator();

        LinkedHashMap<String,String> currentExternalRow;        
        String currentParentTableName = null;
        int currentKeySeq;
        ORelationship currentRelationship;
        OForeignKey currentFk;
        OPrimaryKey currentPk;

        while(it1.hasNext()) {
          currentExternalRow = it1.next();

          // current row has Key_Seq equals to '2' then alghoritm is finished and is stopped
          if(currentExternalRow.get("key_seq").equals("2")) {
            break;
          }

          // the original relationship is fetched from the record through the 'parent table' and the 'key sequence numbers'
          currentParentTableName = currentExternalRow.get("pktable_name");
          currentKeySeq = Integer.parseInt(currentExternalRow.get("key_seq"));

          // building each single relationship from each correspondent foreign key
          currentRelationship = new ORelationship(currentForeignEntityName, currentParentTableName);
          currentFk = new OForeignKey(currentForeignEntity);
          while(it2.hasNext()) {
            LinkedHashMap<String,String> row = it2.next();
            if(row.get("pktable_name").equals(currentParentTableName) && Integer.parseInt(row.get("key_seq")) == currentKeySeq) {
              currentFk.addAttribute(currentForeignEntity.getAttributeByName((String) row.get("fkcolumn_name")));
              it2.remove();
            }
            currentKeySeq++;

          }

          // iterator reset
          it2 = currentEntityRelationships2.iterator();

          // searching correspondent primary key
          currentPk = this.dataBaseSchema.getEntityByName(currentParentTableName).getPrimaryKey();

          // adding foreign key to the entity and the relationship, and adding the foreign key to the 'foreign entity'
          currentRelationship.setPrimaryKey(currentPk);
          currentRelationship.setForeignKey(currentFk);
          currentForeignEntity.getForeignKeys().add(currentFk);

          // adding the relationship to the db schema
          this.dataBaseSchema.getRelationships().add(currentRelationship);
          // adding relationship to the current entity
          currentForeignEntity.getRelationships().add(currentRelationship);

        }

        iteration++;
        OLogManager.instance().info(this, "Relationships from %s built.\n", currentForeignEntityName);
      }

    }catch(SQLException e) {
      e.printStackTrace();
    }finally {
      try {
        if(connection != null) {
          connection.close();
        }
      }catch(SQLException e) {
        e.printStackTrace();
      }
    }

    try {
      if(connection.isClosed())
        OLogManager.instance().info(this, "Connection to DB closed.\n", (Object[])null);
      else
        OLogManager.instance().warn(this, "Connection to DB not closed.\n", (Object[])null);
    }catch(SQLException e) {
      e.printStackTrace();
    }
  }


  private boolean isPresentInResultPrimaryKeys(ResultSet resultPrimaryKeys, String attributeName) throws SQLException {

    while(resultPrimaryKeys.next()) {
      if(resultPrimaryKeys.getString(4).equals(attributeName))
        return true;
    }

    return false;   
  }


  private List<LinkedHashMap<String,String>> fromResultSetToList(ResultSet resultForeignKeys) {
    List<LinkedHashMap<String, String>> rows = new LinkedList<LinkedHashMap<String,String>>();

    try{
      int columnsAmount = resultForeignKeys.getMetaData().getColumnCount();

      while(resultForeignKeys.next()) {
        LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();
        for(int i=1; i<=columnsAmount; i++) {
          row.put(resultForeignKeys.getMetaData().getColumnName(i).toLowerCase(), resultForeignKeys.getString(i));
        }
        rows.add(row);
      }
    }catch(SQLException e) {
      OLogManager.instance().error(this, "%s", e.getMessage());
      e.printStackTrace();
    }
    return rows;
  }


  @Override
  public void buildGraphModel(ONameResolver nameResolver) {

    this.graphModel = new OGraphModel();

    /*
     *  Vertex-type building
     */

    OVertexType currentVertexType;
    String currentVertexTypeName;
    OAttributeProperties attributeProperties = null;

    int numberOfVertexType = this.dataBaseSchema.getEntities().size();
    int iteration = 1;
    for(OEntity currentEntity: this.dataBaseSchema.getEntities()) {

      OLogManager.instance().info(this, "Building '%s' vertex-type (%s/%s)...", currentEntity.getName(), iteration, numberOfVertexType);

      // buildind correspondent vertex-type
      currentVertexTypeName = nameResolver.resolveVertexName(currentEntity.getName());
      currentVertexType = new OVertexType(currentVertexTypeName);

      // TO CHANGE
      if(currentEntity.isJunctionEntity())
        currentVertexType.setFromMany2Many(true);
      else
        currentVertexType.setFromMany2Many(false);

      // adding attributes to vertex-type
      for(OAttribute attribute: currentEntity.getAttributes()) {               
        attributeProperties = new OAttributeProperties(attribute.getOrdinalPosition(), attribute.getDataType(), currentEntity.getPrimaryKey().getInvolvedAttributes().contains(attribute));
        currentVertexType.getAttributeName2attributeProperties().put(nameResolver.resolveVertexProperty(attribute.getName()), attributeProperties);
      }

      // adding vertex to the graph model
      this.graphModel.getVerticesType().add(currentVertexType);

      // rules updating
      this.entity2vertexType.put(currentEntity, currentVertexType);

      iteration++;
      OLogManager.instance().info(this, "Vertex-type %s built.\n", currentVertexType.getType());
    }

    /*
     *  Edge-type building
     */

    OEdgeType currentEdgeType = null;
    String edgeType = null;
    OVertexType currentOutVertex;
    OVertexType currentInVertex;

    int numberOfEdgeType = this.dataBaseSchema.getRelationships().size();
    iteration = 1;
    for(ORelationship relationship: this.dataBaseSchema.getRelationships()) {  
      currentOutVertex = this.graphModel.getVertexByType(nameResolver.resolveVertexName(relationship.getForeignEntityName()));
      currentInVertex = this.graphModel.getVertexByType(nameResolver.resolveVertexName(relationship.getParentEntityName()));
      OLogManager.instance().info(this, "Building edge-type from '%s' to '%s' (%s/%s)...", currentOutVertex.getType(), currentInVertex.getType(), iteration, numberOfEdgeType);

      if(currentOutVertex != null && currentInVertex != null) {
        edgeType = nameResolver.resolveEdgeName(relationship, this);
//        currentEdgeType = new OEdgeType(edgeType, currentOutVertex, currentInVertex);
        
        // if the class edge doesn't exists, it will be created
        currentEdgeType = this.graphModel.getEdgeTypeByName(edgeType);
        if(currentEdgeType == null) {
          currentEdgeType = new OEdgeType(edgeType);
          this.graphModel.addEdgeType(currentEdgeType);
        }
          
        // adding the edge to the two vertices
        currentOutVertex.getOutEdgesType().add(currentEdgeType);
        currentInVertex.getInEdgesType().add(currentEdgeType);
      }
      else {
        OLogManager.instance().error(this, "Error during graph model building phase: vertices-edges information loss, relationship missed.\n", (Object[])null);
      }
      // rules updating
      this.relationship2edgeType.put(relationship, currentEdgeType);

      iteration++;
      OLogManager.instance().info(this, "Edge-type %s built.\n", currentEdgeType.getType());
    }
  }

  
  public void Many2ManyAggregation() {

    OEdgeType newAggregatorEdge;
    OEdgeType currentOutEdge1;
    OEdgeType currentOutEdge2;
    OVertexType outVertexType;
    OVertexType inVertexType;
    String edgeType;

    Iterator<OVertexType> iter = this.graphModel.getVerticesType().iterator();
    OVertexType currentVertex;

    OLogManager.instance().debug(this, "\n\nMany-To-Many aggregation phase:\n", (Object[]) null);

    while(iter.hasNext()) {
      currentVertex = iter.next();

      // if vertex is obtained from a many-to-many relationship, then aggregation
      if(currentVertex.isFromJunctionEntity()) {    

        // building new edge
        currentOutEdge1 = currentVertex.getOutEdgesType().get(0);
        outVertexType = currentOutEdge1.getInVertexType();       
        currentOutEdge2 = currentVertex.getOutEdgesType().get(1); 
        inVertexType = currentOutEdge2.getInVertexType();       
        edgeType = outVertexType.getType() + "2" + inVertexType.getType();
        newAggregatorEdge = new OEdgeType(edgeType, outVertexType, inVertexType);       

        // adding to the edge all properties not belonging to the primary key
        for(String property: currentVertex.getAttributeName2attributeProperties().keySet()) {

          // if property does not belong to the primary key
          if(!currentVertex.getAttributeName2attributeProperties().get(property).isFromPrimaryKey()) {
            newAggregatorEdge.getAttributeName2attributeProperties().put(property, currentVertex.getAttributeName2attributeProperties().get(property));
          }
        }

        // removing old edges from graph model and vertices' "in-edges" collection
        this.graphModel.getEdgesType().remove(currentOutEdge1);
        this.graphModel.getEdgesType().remove(currentOutEdge2);
        outVertexType.getInEdgesType().remove(currentOutEdge1);
        inVertexType.getInEdgesType().remove(currentOutEdge2);

        // removing old vertex
        iter.remove();

        // adding new edge to graph model
        this.graphModel.getEdgesType().add(newAggregatorEdge);

        // adding new edge to the vertices' "in/out-edges" collections
        outVertexType.getOutEdgesType().add(newAggregatorEdge);
        inVertexType.getInEdgesType().add(newAggregatorEdge);
      }
    }
  }


  public ODataBaseSchema getDataBaseSchema() {
    return this.dataBaseSchema;
  }


  public void setDataBaseSchema(ODataBaseSchema dataBaseSchema) {
    this.dataBaseSchema = dataBaseSchema;
  }


  public OGraphModel getGraphModel() {
    return this.graphModel;
  }


  public void setGraphModel(OGraphModel graphModel) {
    this.graphModel = graphModel;
  }


  public Map<OEntity, OVertexType> getEntity2vertexType() {
    return this.entity2vertexType;
  }


  public void setEntity2vertexType(Map<OEntity, OVertexType> entity2vertexType) {
    this.entity2vertexType = entity2vertexType;
  }


  public Map<ORelationship, OEdgeType> getRelationship2edgeType() {
    return this.relationship2edgeType;
  }


  public void setRelationship2edgeType(Map<ORelationship, OEdgeType> relationship2edgeTypeRules) {
    this.relationship2edgeType = relationship2edgeTypeRules;
  }

  public OVertexType getVertexTypeByName(String name) {

    for(OVertexType currentVertexType: this.entity2vertexType.values()) {
      if(currentVertexType.getType().equals(name)) {
        return currentVertexType;
      }
    }
    return null;
  }

//  private OEdgeType getEdgeTypeByName(String name) {
//
//    for(OEdgeType currentEdgeType: this.relationship2edgeType.values()) {
//      if(currentEdgeType.getType().equalsIgnoreCase(name)) {
//        return currentEdgeType;
//      }
//    }    
//    return null;
//  }


  public Map<String,Integer> getEdgeTypeName2count() {
    return this.edgeTypeName2count;
  }


  public void setEdgeTypeName2count(Map<String, Integer> edgeTypeName2count) {
    this.edgeTypeName2count = edgeTypeName2count;
  }


  public String toString() {

    String s = "\n\n\n------------------------------ MAPPER DESCRIPTION ------------------------------\n\n\n";
    s += "RULES\n\n";
    s += "- Entity2VertexType Rules:\n\n";
    for(OEntity entity: this.entity2vertexType.keySet()) {
      s += entity.getName() + " --> " + this.entity2vertexType.get(entity).getType() + "\n";
    }
    s += "\n\n- Relaionship2EdgeType Rules:\n\n";
    for(ORelationship relationship: this.relationship2edgeType.keySet()) {
      s += relationship.getForeignEntityName() + "2" + relationship.getParentEntityName() + " --> " + this.relationship2edgeType.get(relationship).getType() + "\n";
    }
    s += "\n\n- EdgeTypeName2Count Rules:\n\n";
    for(String edgeName: this.edgeTypeName2count.keySet()) {
      s += edgeName + " --> " + this.edgeTypeName2count.get(edgeName) + "\n";
    }
    s += "\n";


    return s;
  }

}
