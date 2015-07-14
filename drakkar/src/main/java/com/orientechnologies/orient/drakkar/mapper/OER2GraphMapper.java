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

package com.orientechnologies.orient.drakkar.mapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.ODrakkarStatistics;
import com.orientechnologies.orient.drakkar.model.dbschema.OAttribute;
import com.orientechnologies.orient.drakkar.model.dbschema.ODataBaseSchema;
import com.orientechnologies.orient.drakkar.model.dbschema.ODataSourceSchema;
import com.orientechnologies.orient.drakkar.model.dbschema.OEntity;
import com.orientechnologies.orient.drakkar.model.dbschema.OForeignKey;
import com.orientechnologies.orient.drakkar.model.dbschema.OPrimaryKey;
import com.orientechnologies.orient.drakkar.model.dbschema.ORelationship;
import com.orientechnologies.orient.drakkar.model.graphmodel.OEdgeType;
import com.orientechnologies.orient.drakkar.model.graphmodel.OElementType;
import com.orientechnologies.orient.drakkar.model.graphmodel.OGraphModel;
import com.orientechnologies.orient.drakkar.model.graphmodel.OModelProperty;
import com.orientechnologies.orient.drakkar.model.graphmodel.OVertexType;
import com.orientechnologies.orient.drakkar.nameresolver.ONameResolver;
import com.orientechnologies.orient.drakkar.persistence.util.ODBSourceConnection;

/**
 * Implementation of OSource2GraphMapper that manages the source DB schema and the destination graph model with their correspondences.
 * It has the responsibility to build in memory the two models: the first is built from the source DB meta-data through the JDBC driver,
 * the second from the source DB schema just created.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OER2GraphMapper extends OSource2GraphMapper {

  protected ODBSourceConnection dbSourceConnection;

  // source model
  protected ODataBaseSchema dataBaseSchema;

  // Rules
  protected Map<OEntity,OVertexType> entity2vertexType;  
  protected Map<ORelationship,OEdgeType> relationship2edgeType;
  protected Map<String,Integer> edgeTypeName2count;
  protected Map<String,OAggregatorEdge> joinVertex2aggregatorEdges;

  public OER2GraphMapper (String driver, String uri, String username, String password) {
    this.dbSourceConnection = new ODBSourceConnection(driver, uri, username, password);
    this.entity2vertexType = new HashMap<OEntity,OVertexType>();
    this.relationship2edgeType = new HashMap<ORelationship,OEdgeType>();
    this.edgeTypeName2count = new TreeMap<String,Integer>();
    this.joinVertex2aggregatorEdges = new HashMap<String, OAggregatorEdge>();
  }


  public void buildSourceSchema(ODrakkarContext context) {

    Connection connection = null;
    ODrakkarStatistics statistics = context.getStatistics();
    statistics.startWork1Time = new Date();
    statistics.runningStepNumber = 1;
    statistics.notifyListeners();

    try {

      connection = this.dbSourceConnection.getConnection(context);
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
      if(this.dbSourceConnection.getDriver().contains("Oracle")) {
        ResultSet schemas = databaseMetaData.getSchemas();
        while(schemas.next()) {
          if(schemas.getString(1).equalsIgnoreCase(this.dbSourceConnection.getUsername())) {
            tableSchemaPattern = schemas.getString(1);
            break;
          }
        }
      }

      String tableNamePattern = null;
      String[] tableTypes = {"TABLE"};

      ResultSet resultTable = null;
      resultTable = databaseMetaData.getTables(tableCatalog, tableSchemaPattern, tableNamePattern, tableTypes);
      ResultSet resultColumns;
      ResultSet resultPrimaryKeys;
      ResultSet resultForeignKeys;

      // Giving db's table names
      while(resultTable.next()) {
        String tableName = resultTable.getString(3);
        tablesName.add(tableName);  
      }

      int numberOfTables = tablesName.size();
      statistics.totalNumberOfEntities = numberOfTables;

      // closing resultTable
      this.closeCursor(resultTable);

      context.getOutputManager().debug(numberOfTables + " tables found.");

      OEntity currentEntity;
      OPrimaryKey pKey;

      List<LinkedHashMap<String,String>> currentEntityRelationships1;
      List<LinkedHashMap<String,String>> currentEntityRelationships2;

      // Variables for records counting
      Statement statement = connection.createStatement();
      String sql;
      ResultSet currentTableRecordAmount;
      int totalNumberOfRecord = 0;


      /*
       *  Entity building
       */

      int iteration = 1;
      for(String currentTableName: tablesName) {

        context.getOutputManager().debug("Building '" + currentTableName + "' entity (" + iteration + "/" + numberOfTables + ")...");

        // Counting current-table's record
        sql = "select count(*) from " + currentTableName;
        currentTableRecordAmount = statement.executeQuery(sql);
        if (currentTableRecordAmount.next()) {
          totalNumberOfRecord += currentTableRecordAmount.getInt(1);
        }
        this.closeCursor(currentTableRecordAmount);

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
        this.closeCursor(resultColumns);
        this.closeCursor(resultPrimaryKeys);

        currentEntity.setPrimaryKey(pKey);

        // adding entity to db schema
        this.dataBaseSchema.getEntities().add(currentEntity);

        iteration++;
        context.getOutputManager().debug("Entity " + currentTableName + " built.\n");
        statistics.builtEntities++;
        statistics.totalNumberOfRecords = totalNumberOfRecord;

      }
      statement.close();


      /*
       *  Building relationships
       */

      iteration = 1;
      for(OEntity currentForeignEntity: this.dataBaseSchema.getEntities()) {

        String currentForeignEntityName = currentForeignEntity.getName();

        context.getOutputManager().debug("Building relationships starting from '" + currentForeignEntityName + "' entity (" + iteration + "/" + numberOfTables + ")...");

        String foreignCatalog = null;
        String foreignSchema = null;

        resultForeignKeys = databaseMetaData.getImportedKeys(foreignCatalog, foreignSchema, currentForeignEntityName);

        // copy of resultset in a HashLinkedMap
        currentEntityRelationships1 = this.fromResultSetToList(resultForeignKeys, context);
        currentEntityRelationships2 = new LinkedList<LinkedHashMap<String,String>>();
        currentEntityRelationships2.addAll(currentEntityRelationships1);

        this.closeCursor(resultForeignKeys);

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

          // current row has Key_Seq equals to '2' then algorithm is finished and is stopped
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
          // updating statistics
          statistics.detectedRelationships += 1;
        }

        iteration++;
        context.getOutputManager().debug("Relationships from " + currentForeignEntityName + " built.\n");
        statistics.doneEntity4Relationship++;

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
        context.getOutputManager().debug("Connection to DB closed.\n");
      else {
        statistics.warningMessages.add("Connection to DB not closed.");
      }      
    }catch(SQLException e) {
      e.printStackTrace();
    }
    statistics.notifyListeners();
    statistics.runningStepNumber = -1;
  }

  /**
   * @param resultTable
   */
  private void closeCursor(ResultSet resultTable) {
    try {
      resultTable.close();
    } catch (SQLException e) {
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


  private List<LinkedHashMap<String,String>> fromResultSetToList(ResultSet resultForeignKeys, ODrakkarContext context) {
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
      context.getOutputManager().error(e.getMessage());
      e.printStackTrace();
    }
    return rows;
  }


  @Override
  public void buildGraphModel(ONameResolver nameResolver, ODrakkarContext context) {

    this.graphModel = new OGraphModel();
    ODrakkarStatistics statistics = context.getStatistics();
    statistics.startWork2Time = new Date();
    statistics.runningStepNumber = 2;


    /*
     *  Vertex-type building
     */

    OVertexType currentVertexType;
    String currentVertexTypeName;
    OModelProperty currentProperty = null;
    OElementType currentParentElement = null;

    int numberOfVertexType = this.dataBaseSchema.getEntities().size();
    statistics.totalNumberOfModelVertices = numberOfVertexType;
    int iteration = 1;
    for(OEntity currentEntity: this.dataBaseSchema.getEntities()) {

      context.getOutputManager().debug("Building '" + currentEntity.getName() + "' vertex-type (" + iteration + "/" + numberOfVertexType + ")...");

      // building correspondent vertex-type
      currentVertexTypeName = nameResolver.resolveVertexName(currentEntity.getName());
      currentVertexType = new OVertexType(currentVertexTypeName);

      // recognizing joint tables of dimension 2
      if(currentEntity.isJoinEntityDim2())
        currentVertexType.setIsFromJoinTable(true);
      else
        currentVertexType.setIsFromJoinTable(false);

      // adding attributes to vertex-type
      for(OAttribute attribute: currentEntity.getAttributes()) {        
        if(currentEntity.getParentEntity() == null) {
          currentProperty = new OModelProperty(nameResolver.resolveVertexProperty(attribute.getName()), attribute.getOrdinalPosition(), attribute.getDataType(), currentEntity.getPrimaryKey().getInvolvedAttributes().contains(attribute));
          currentVertexType.getProperties().add(currentProperty);
        }
        // INHERITANCE CASE: id of the subclass is not added, because the id of the superclass is inherited
        else {
          OAttribute inheritedPrimaryKeyAttribute;
          if(currentEntity.getPrimaryKey() != null && currentEntity.getPrimaryKey().getInvolvedAttributes().contains(attribute)) {
            inheritedPrimaryKeyAttribute = currentEntity.getParentEntity().getPrimaryKey().getAttributeByOrdinalPosition(attribute.getOrdinalPosition());
            currentProperty = new OModelProperty(nameResolver.resolveVertexProperty(inheritedPrimaryKeyAttribute.getName()), inheritedPrimaryKeyAttribute.getOrdinalPosition(), inheritedPrimaryKeyAttribute.getDataType(), true);
            currentVertexType.getInheritedProperties().add(currentProperty);
          }
          else {
            
            // patch
            if(currentEntity.getPrimaryKey() != null)
            currentProperty = new OModelProperty(nameResolver.resolveVertexProperty(attribute.getName()), attribute.getOrdinalPosition(), attribute.getDataType(), currentEntity.getPrimaryKey().getInvolvedAttributes().contains(attribute));
            else
              currentProperty = new OModelProperty(nameResolver.resolveVertexProperty(attribute.getName()), attribute.getOrdinalPosition(), attribute.getDataType(), false);

            currentVertexType.getProperties().add(currentProperty);
          }
        }
      }

      // adding parent vertex if the corresponding entity has a parent
      if(currentEntity.getParentEntity() != null) {
        currentParentElement = this.graphModel.getVertexByNameIgnoreCase(currentEntity.getParentEntity().getName());
        currentVertexType.setParentType(currentParentElement);
        currentVertexType.setInheritanceLevel(currentEntity.getInheritanceLevel());
      }

      // adding vertex to the graph model
      this.graphModel.getVerticesType().add(currentVertexType);

      // rules updating
      this.entity2vertexType.put(currentEntity, currentVertexType);

      iteration++;
      context.getOutputManager().debug("Vertex-type " + currentVertexType.getName() + " built.\n");
      statistics.builtModelVertexTypes++;
    }

    // sorting vertices type for inheritance level and then for name
    Collections.sort(this.graphModel.getVerticesType());


    /*
     *  Edge-type building
     */

    OEdgeType currentEdgeType = null;
    String edgeType = null;
    OVertexType currentOutVertex;
    OVertexType currentInVertex;

    int numberOfEdgeType = this.dataBaseSchema.getRelationships().size();
    statistics.totalNumberOfRelationships = numberOfEdgeType;
    iteration = 1;

    // edges added through relationships (foreign keys of db)
    for(ORelationship relationship: this.dataBaseSchema.getRelationships()) {  
      currentOutVertex = this.graphModel.getVertexByName(nameResolver.resolveVertexName(relationship.getForeignEntityName()));
      currentInVertex = this.graphModel.getVertexByName(nameResolver.resolveVertexName(relationship.getParentEntityName()));
      context.getOutputManager().debug("Building edge-type from '" + currentOutVertex.getName() + "' to '" + currentInVertex.getName() + "' (" + iteration + "/" + numberOfEdgeType + ")...");

      if(currentOutVertex != null && currentInVertex != null) {

        // in case of inheritance no edge type is adopted, otherwise a new edge type is added to the graph model
        if(currentOutVertex.getParentType() == null) {

          // if the class edge doesn't exists, it will be created
          edgeType = nameResolver.resolveEdgeName(relationship);

          currentEdgeType = this.graphModel.getEdgeTypeByName(edgeType);
          if(currentEdgeType == null) {
            currentEdgeType = new OEdgeType(edgeType, null, currentInVertex);  // TO UPDATE !!!!!!!!
            this.graphModel.getEdgesType().add(currentEdgeType);
            context.getOutputManager().debug("Edge-type " + currentEdgeType.getName() + " built.\n");
            statistics.builtModelEdgeTypes++;
          }


          // adding the edge to the two vertices
          currentOutVertex.getOutEdgesType().add(currentEdgeType);
          currentInVertex.getInEdgesType().add(currentEdgeType);
        }
      }
      else {
        context.getOutputManager().error("Error during graph model building phase: vertices-edges information loss, relationship missed.\n");
      }
      // rules updating
      this.relationship2edgeType.put(relationship, currentEdgeType);

      iteration++;
      statistics.analizedRelationships++;
    }

    //    // edges added through inheritance (no foreign key present in the db)
    //    OVertexType currentParentVertex;
    //    for(OVertexType currentVertex: this.graphModel.getVerticesType()) {
    //      
    //      if(currentVertex.getParentType() != null) {
    //        currentParentVertex = (OVertexType) currentVertex.getParentType();
    //        
    //        // if edge "IsAs" is not present, it is added
    //        if(!currentVertex.hasEdgeType("IsAs", Direction.OUT)) {
    //          
    //          currentEdgeType = this.graphModel.getEdgeTypeByName("IsAs");
    //          if(currentEdgeType == null) {
    //            currentEdgeType = new OEdgeType(edgeType, null, currentParentVertex);  // TO UPDATE !!!!!!!!
    //            this.graphModel.getEdgesType().add(currentEdgeType);
    //            context.getOutputManager().debug("Edge-type " + currentEdgeType.getName() + " built.\n");
    //            statistics.builtModelEdgeTypes++;
    //          }
    //          
    //          // adding the edge to the two vertices
    //          currentVertex.getOutEdgesType().add(currentEdgeType);
    //          currentParentVertex.getInEdgesType().add(currentEdgeType);
    //        }
    //      }
    //      
    //    }

    statistics.notifyListeners();
    statistics.runningStepNumber = -1;
  }


  public void JoinTableDim2Aggregation(ODrakkarContext context) {

    OEdgeType newAggregatorEdge;
    OEdgeType currentOutEdge1;
    OEdgeType currentOutEdge2;
    OVertexType outVertexType;
    OVertexType inVertexType;
    String edgeType;

    Iterator<OVertexType> iter = this.graphModel.getVerticesType().iterator();
    OVertexType currentVertex;

    context.getOutputManager().debug("\n\nJoin Table aggregation phase:\n");

    while(iter.hasNext()) {
      currentVertex = iter.next();

      // if vertex is obtained from a join table of dimension 2,
      // then aggregation is performed
      if(currentVertex.isFromJoinTable() && currentVertex.getOutEdgesType().size() == 2) { 

        // building new edge
        currentOutEdge1 = currentVertex.getOutEdgesType().get(0);
        outVertexType = currentOutEdge1.getInVertexType();       
        currentOutEdge2 = currentVertex.getOutEdgesType().get(1); 
        inVertexType = currentOutEdge2.getInVertexType();       
        edgeType = currentOutEdge2.getName();
        newAggregatorEdge = new OEdgeType(edgeType, null, inVertexType);     // TO UPDATE  

        // adding to the edge all properties not belonging to the primary key
        for(OModelProperty currentProperty: currentVertex.getProperties()) {

          // if property does not belong to the primary key
          if(!currentProperty.isFromPrimaryKey()) {
            newAggregatorEdge.getProperties().add(currentProperty);
          }
        }

        // adding to the edge all properties belonging to the old edges
        for(OModelProperty currentProperty: currentOutEdge1.getProperties()) {
          newAggregatorEdge.getProperties().add(currentProperty);
        }
        for(OModelProperty currentProperty: currentOutEdge2.getProperties()) {
          newAggregatorEdge.getProperties().add(currentProperty);
        }


        // removing old edges from graph model and from vertices' "in-edges" collection
        this.graphModel.getEdgesType().remove(currentOutEdge1);
        this.graphModel.getEdgesType().remove(currentOutEdge2);
        outVertexType.getInEdgesType().remove(currentOutEdge1);
        inVertexType.getInEdgesType().remove(currentOutEdge2);

        // adding entry to the map
        this.joinVertex2aggregatorEdges.put(currentVertex.getName(), new OAggregatorEdge(outVertexType.getName(), inVertexType.getName(), newAggregatorEdge.getName()));

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

  public ODataSourceSchema getSourceSchema() {
    return this.getDataBaseSchema();
  }

  public ODataBaseSchema getDataBaseSchema() {
    return this.dataBaseSchema;
  }


  public void setDataBaseSchema(ODataBaseSchema dataBaseSchema) {
    this.dataBaseSchema = dataBaseSchema;
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
      if(currentVertexType.getName().equals(name)) {
        return currentVertexType;
      }
    }
    return null;
  }

  public Map<String,Integer> getEdgeTypeName2count() {
    return this.edgeTypeName2count;
  }


  public void setEdgeTypeName2count(Map<String, Integer> edgeTypeName2count) {
    this.edgeTypeName2count = edgeTypeName2count;
  }

  public Map<String, OAggregatorEdge> getJoinVertex2aggregatorEdges() {
    return joinVertex2aggregatorEdges;
  }


  public void setJoinVertex2aggregatorEdges(Map<String, OAggregatorEdge> joinVertex2aggregatorEdges) {
    this.joinVertex2aggregatorEdges = joinVertex2aggregatorEdges;
  }


  public String toString() {

    String s = "\n\n\n------------------------------ MAPPER DESCRIPTION ------------------------------\n\n\n";
    s += "RULES\n\n";
    s += "- Entity2VertexType Rules:\n\n";
    for(OEntity entity: this.entity2vertexType.keySet()) {
      s += entity.getName() + " --> " + this.entity2vertexType.get(entity).getName() + "\n";
    }
    s += "\n\n- Relaionship2EdgeType Rules:\n\n";
    for(ORelationship relationship: this.relationship2edgeType.keySet()) {
      s += relationship.getForeignEntityName() + "2" + relationship.getParentEntityName() + " --> " + this.relationship2edgeType.get(relationship).getName() + "\n";
    }
    s += "\n\n- EdgeTypeName2Count Rules:\n\n";
    for(String edgeName: this.edgeTypeName2count.keySet()) {
      s += edgeName + " --> " + this.edgeTypeName2count.get(edgeName) + "\n";
    }
    s += "\n";


    return s;
  }

}
