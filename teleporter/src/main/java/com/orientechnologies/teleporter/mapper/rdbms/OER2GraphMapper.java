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

package com.orientechnologies.teleporter.mapper.rdbms;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.*;
import com.orientechnologies.teleporter.model.graphmodel.*;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.util.ODBSourceConnection;

import java.sql.*;
import java.util.*;
import java.util.Date;

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
  protected Map<OEntity,OVertexType>      entity2vertexType;
  protected Map<OVertexType,OEntity>      vertexType2entity;
  protected Map<ORelationship,OEdgeType>  relationship2edgeType;
  protected Map<OEdgeType, ORelationship> edgeType2relationship;
  protected Map<String,Integer>           edgeTypeName2count;
  protected Map<OVertexType,OAggregatorEdge>   joinVertex2aggregatorEdges;

  // filters
  protected List<String> includedTables;
  protected List<String> excludedTables;

  // supplementary configuration
  protected ODocument configuration;


  public OER2GraphMapper (String driver, String uri, String username, String password, List<String> includedTables, List<String> excludedTables, ODocument configuration) {
    this.dbSourceConnection = new ODBSourceConnection(driver, uri, username, password);
    this.entity2vertexType = new LinkedHashMap<OEntity,OVertexType>();
    this.vertexType2entity = new LinkedHashMap<OVertexType,OEntity>();
    this.relationship2edgeType = new LinkedHashMap<ORelationship,OEdgeType>();
    this.edgeType2relationship = new LinkedHashMap<OEdgeType,ORelationship>();
    this.edgeTypeName2count = new TreeMap<String,Integer>();
    this.joinVertex2aggregatorEdges = new LinkedHashMap<OVertexType, OAggregatorEdge>();

    if(includedTables != null)
      this.includedTables = includedTables;
    else
      this.includedTables = new ArrayList<String>();

    if(excludedTables != null)
      this.excludedTables = excludedTables;

    else
      this.excludedTables = new ArrayList<String>();
    this.configuration = configuration;

    // creating the two empty models
    this.dataBaseSchema = new ODataBaseSchema();
    this.graphModel = new OGraphModel();

  }

  public void upsertEntityVertexRules(OVertexType currentVertexType, OEntity currentEntity) {
    this.entity2vertexType.put(currentEntity, currentVertexType);
    this.vertexType2entity.put(currentVertexType, currentEntity);
  }

  public void upsertRelationshipEdgeRules(ORelationship currentRelationship, OEdgeType currentEdgeType) {
    this.relationship2edgeType.put(currentRelationship, currentEdgeType);
    this.edgeType2relationship.put(currentEdgeType, currentRelationship);
  }

  /**
   * MACRO EXECUTION BLOCK: BUILD SOURCE DATABASE SCHEMA
   * Builds the database schema and the rules for the mapping with the graph model through 3 micro execution blocks:
   *  - Build Entities
   *  - Build Out-Relationships
   *  - Build In-Relationships
   *
   * @param context
   */

  public void buildSourceDatabaseSchema(OTeleporterContext context) {

    Connection connection = null;
    OTeleporterStatistics statistics = context.getStatistics();
    statistics.startWork1Time = new Date();
    statistics.runningStepNumber = 1;
    statistics.notifyListeners();

    try {

      connection = this.dbSourceConnection.getConnection(context);
      DatabaseMetaData databaseMetaData = connection.getMetaData();

      /*
       *  General DB Info and table filtering
       */

      int majorVersion = databaseMetaData.getDatabaseMajorVersion();
      int minorVersion = databaseMetaData.getDatabaseMinorVersion();
      int driverMajorVersion = databaseMetaData.getDriverMajorVersion();
      int driverMinorVersion = databaseMetaData.getDriverMinorVersion();
      String productName = databaseMetaData.getDatabaseProductName();
      String productVersion = databaseMetaData.getDatabaseProductVersion();

      this.dataBaseSchema.setMajorVersion(majorVersion);
      this.dataBaseSchema.setMinorVersion(minorVersion);
      this.dataBaseSchema.setDriverMajorVersion(driverMajorVersion);
      this.dataBaseSchema.setDriverMinorVersion(driverMinorVersion);
      this.dataBaseSchema.setProductName(productName);
      this.dataBaseSchema.setProductVersion(productVersion);

      /*
       *  Entity building
       */

      int numberOfTables = this.buildEntities(databaseMetaData, connection, context);

      /*
       *  Building Out-relationships
       */

      buildOutRelationships(databaseMetaData, numberOfTables, context);

      // Adding/updating relationships in the manual configuration
//      if(this.configuration != null) {
//        this.applyImportConfiguration(context);
//      }

      /*
       *  Building In-relationships
       */

      buildInRelationships(context);


    } catch (SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }finally {
      try {
        if(connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        String mess = "";
        context.printExceptionMessage(e, mess, "error");
        context.printExceptionStackTrace(e, "debug");
      }
    }

    try {
      if(connection.isClosed())
        context.getOutputManager().debug("\nConnection to DB closed.\n");
      else {
        statistics.warningMessages.add("\nConnection to DB not closed.\n");
      }
    } catch (SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
    statistics.notifyListeners();
    statistics.runningStepNumber = -1;
  }


  /**
   * MICRO EXECUTION BLOCK: BUILD SOURCE DATABASE SCHEMA - BUILD ENTITIES
   * Builds the Entities starting from the source database metadata.
   *
   * @param databaseMetaData
   * @param sourceDBConnection
   * @param context
   * @return
   * @throws SQLException
   */

  private int buildEntities(DatabaseMetaData databaseMetaData, Connection sourceDBConnection, OTeleporterContext context) throws SQLException {

    OTeleporterStatistics statistics = context.getStatistics();
    String quote = context.getQueryQuote();
    Map<String,String> tablesName2schema = new LinkedHashMap<String,String>();

    String tableCatalog = null;
    String tableSchemaPattern = null;
    String tableNamePattern = null;
    String[] tableTypes = {"TABLE"};
    if(this.dbSourceConnection.getDriver().contains("Oracle")) {
      ResultSet schemas = databaseMetaData.getSchemas();
      while(schemas.next()) {
        if(schemas.getString(1).equalsIgnoreCase(this.dbSourceConnection.getUsername())) {
          tableSchemaPattern = schemas.getString(1);
          break;
        }
      }
    }
    ResultSet resultTable = databaseMetaData.getTables(tableCatalog, tableSchemaPattern, tableNamePattern, tableTypes);

    // Giving db's table names
    while(resultTable.next()) {
      String tableSchema = resultTable.getString("TABLE_SCHEM");
      String tableName = resultTable.getString("TABLE_NAME");

      if(this.isTableAllowed(tableName))  // filtering tables according to "include-list" and "exclude-list"
        tablesName2schema.put(tableName,tableSchema);
    }

    int numberOfTables = tablesName2schema.size();
    statistics.totalNumberOfEntities = numberOfTables;

    // closing resultTable
    this.closeCursor(resultTable, context);
    context.getOutputManager().debug("\n%s tables found.\n", numberOfTables);

    // Variables for records counting
    Statement statement = sourceDBConnection.createStatement();
    int totalNumberOfRecord = 0;

    int iteration = 1;
    for(String currentTableName: tablesName2schema.keySet()) {

      context.getOutputManager().debug("\nBuilding '%s' entity (%s/%s)...\n", currentTableName, iteration, numberOfTables);

      // Counting current-table's record
      String currentTableSchema = tablesName2schema.get(currentTableName);
      String sql;
      if(currentTableSchema != null)
        sql = "select count(*) from " + currentTableSchema + "." + quote + currentTableName + quote;
      else
        sql = "select count(*) from " + quote + currentTableName + quote;

      ResultSet currentTableRecordAmount = statement.executeQuery(sql);
      if (currentTableRecordAmount.next()) {
        totalNumberOfRecord += currentTableRecordAmount.getInt(1);
      }
      this.closeCursor(currentTableRecordAmount, context);

      // creating entity
      OEntity currentEntity = new OEntity(currentTableName, currentTableSchema);

      // adding attributes and primary keys
      OPrimaryKey pKey = new OPrimaryKey(currentEntity);

      String columnCatalog = null;
      String columnSchemaPattern = null;
      String columnNamePattern = null;
      String primaryKeyCatalog = null;
      String primaryKeySchema = currentTableSchema;

      ResultSet resultColumns = databaseMetaData.getColumns(columnCatalog, columnSchemaPattern, currentTableName, columnNamePattern);
      ResultSet resultPrimaryKeys = databaseMetaData.getPrimaryKeys(primaryKeyCatalog, primaryKeySchema, currentTableName);

      List<String> currentPrimaryKeys = this.getPrimaryKeysFromResulset(resultPrimaryKeys);

      while(resultColumns.next()) {
        OAttribute currentAttribute = new OAttribute(resultColumns.getString("COLUMN_NAME"), resultColumns.getInt("ORDINAL_POSITION"), resultColumns.getString("TYPE_NAME"), currentEntity);
        currentEntity.addAttribute(currentAttribute);

        // if the current attribute is involved in the primary key, it will be added to the attributes of pKey.
        if(currentPrimaryKeys.contains(currentAttribute.getName())) {
          pKey.addAttribute(currentAttribute);
        }
      }
      this.closeCursor(resultColumns, context);
      this.closeCursor(resultPrimaryKeys, context);

      currentEntity.setPrimaryKey(pKey);

      // if the primary key doesn't involve any attribute, a warning message is generated
      if(pKey.getInvolvedAttributes().size() == 0)
        context.getStatistics().warningMessages.add("It's not declared a primary key for the Entity " + currentEntity.getName() + ", this might lead to issues during the migration or the sync executions "
                + "(the first importing is quite safe).");

      // adding entity to db schema
      this.dataBaseSchema.getEntities().add(currentEntity);

      iteration++;
      context.getOutputManager().debug("\nEntity %s built.\n", currentTableName);
      statistics.builtEntities++;
      statistics.totalNumberOfRecords = totalNumberOfRecord;
    }
    statement.close();

    return numberOfTables;
  }


  /**
   * MICRO EXECUTION BLOCK: BUILD SOURCE DATABASE SCHEMA - BUILD OUT-RELATIONSHIPS
   * Builds the references to the "Out Relationships" starting from the source database metadata.
   *
   * @param databaseMetaData
   * @param numberOfTables
   * @param context
   * @throws SQLException
   */

  private void buildOutRelationships(DatabaseMetaData databaseMetaData, int numberOfTables, OTeleporterContext context) throws SQLException {

    OTeleporterStatistics statistics = context.getStatistics();

    int iteration = 1;
    for(OEntity currentForeignEntity: this.dataBaseSchema.getEntities()) {

      String currentForeignEntityName = currentForeignEntity.getName();
      String foreignSchema = currentForeignEntity.getSchemaName();
      context.getOutputManager().debug("\nBuilding OUT relationships starting from '%s' entity (%s/%s)...\n", currentForeignEntityName, iteration, numberOfTables);

      String foreignCatalog = null;
      ResultSet resultForeignKeys = databaseMetaData.getImportedKeys(foreignCatalog, foreignSchema, currentForeignEntityName);

      // copy of Resultset in a HashLinkedMap
      List<LinkedHashMap<String, String>> currentEntityRelationships1 = this.fromResultSetToList(resultForeignKeys, context);
      List<LinkedHashMap<String, String>> currentEntityRelationships2 = new LinkedList<LinkedHashMap<String,String>>();

      for(LinkedHashMap<String,String> row: currentEntityRelationships1) {
        currentEntityRelationships2.add(row);
      }

      this.closeCursor(resultForeignKeys, context);

      Iterator<LinkedHashMap<String,String>> it1 = currentEntityRelationships1.iterator();
      Iterator<LinkedHashMap<String,String>> it2 = currentEntityRelationships2.iterator();

      while(it1.hasNext()) {
        LinkedHashMap<String,String> currentExternalRow = it1.next();

        // current row has Key_Seq equals to '2' then algorithm is finished and is stopped
        if(currentExternalRow.get("key_seq").equals("2")) {
          break;
        }

        // the original relationship is fetched from the record through the 'parent table' and the 'key sequence numbers'
        String currentParentTableName = currentExternalRow.get("pktable_name");
        int currentKeySeq = Integer.parseInt(currentExternalRow.get("key_seq"));

        // building each single relationship from each correspondent foreign key
        ORelationship currentRelationship = new ORelationship(currentForeignEntityName, currentParentTableName);
        OForeignKey currentFk = new OForeignKey(currentForeignEntity);
        while(it2.hasNext()) {
          LinkedHashMap<String,String> row = it2.next();
          if(row.get("pktable_name").equals(currentParentTableName) && Integer.parseInt(row.get("key_seq")) == currentKeySeq) {
            currentFk.addAttribute(currentForeignEntity.getAttributeByName((String) row.get("fkcolumn_name")));
            it2.remove();
            currentKeySeq++;
          }
        }

        // iterator reset
        it2 = currentEntityRelationships2.iterator();

        // searching correspondent primary key
        OPrimaryKey currentPk = this.dataBaseSchema.getEntityByName(currentParentTableName).getPrimaryKey();

        // adding foreign key to the entity and the relationship, and adding the foreign key to the 'foreign entity'
        currentRelationship.setPrimaryKey(currentPk);
        currentRelationship.setForeignKey(currentFk);
        currentForeignEntity.getForeignKeys().add(currentFk);

        // adding the relationship to the db schema
        this.dataBaseSchema.getRelationships().add(currentRelationship);
        // adding relationship to the current entity
        currentForeignEntity.getOutRelationships().add(currentRelationship);
        // updating statistics
        statistics.detectedRelationships += 1;
      }

      iteration++;
      context.getOutputManager().debug("\nOUT Relationships from %s built.\n", currentForeignEntityName);
      statistics.doneEntity4Relationship++;
      statistics.totalNumberOfRelationships = this.dataBaseSchema.getRelationships().size();

    }
  }


  /**
   * MICRO EXECUTION BLOCK: BUILD SOURCE DATABASE SCHEMA - BUILD IN-RELATIONSHIPS
   * Builds the references to the "In Relationships" starting from the references to the "Out Relationships".
   *
   * @param context
   */

  private void buildInRelationships(OTeleporterContext context) {

    int iteration = 1;
    context.getOutputManager().debug("\nConnecting IN relationships...\n");

    for(ORelationship currentRelationship: this.dataBaseSchema.getRelationships()) {
      OEntity currentInEntity = this.getDataBaseSchema().getEntityByName(currentRelationship.getParentEntityName());
      currentInEntity.getInRelationships().add(currentRelationship);
    }
    context.getOutputManager().debug("\nIN relationships built.\n");
  }


  private List<String> getPrimaryKeysFromResulset(ResultSet resultPrimaryKeys) throws SQLException {

    List<String> currentPrimaryKeys = new LinkedList<String>();

    while(resultPrimaryKeys.next()) {
      currentPrimaryKeys.add(resultPrimaryKeys.getString(4));
    }
    return currentPrimaryKeys;
  }


  /**
   * @param result
   */
  private void closeCursor(ResultSet result, OTeleporterContext context) {
    try {
      if(result != null)
        result.close();
    } catch (SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "debug");
    }
  }


  /*
   * Transforms a ResultSet in a List, filtering relationships according to "include/exclude-lists"
   */

  private List<LinkedHashMap<String,String>> fromResultSetToList(ResultSet resultForeignKeys, OTeleporterContext context) {

    List<LinkedHashMap<String, String>> rows = new LinkedList<LinkedHashMap<String,String>>();

    try{
      int columnsAmount = resultForeignKeys.getMetaData().getColumnCount();

      while(resultForeignKeys.next()) {

        if(this.isTableAllowed(resultForeignKeys.getString("pktable_name")) && this.dataBaseSchema.getEntityByName(resultForeignKeys.getString("pktable_name")) != null) {
          //          if(this.isTableAllowed(resultForeignKeys.getString("pktable_name")) && this.dataBaseSchema.getEntityByName(resultForeignKeys.getString("pktable_name")) != null) {

          LinkedHashMap<String,String> row = new LinkedHashMap<String,String>();
          for(int i=1; i<=columnsAmount; i++) {
            row.put(resultForeignKeys.getMetaData().getColumnName(i).toLowerCase(), resultForeignKeys.getString(i));
          }
          rows.add(row);
        }
      }
    }catch(SQLException e) {
      String mess = "";
      context.printExceptionMessage(e, mess, "error");
      context.printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }
    return rows;
  }


  @Override
  /**
   * MACRO EXECUTION BLOCK: BUILD GRAPH MODEL
   * Builds the graph model and the rules for the mapping with the source database schema through 2 micro execution blocks:
   *  - Build Vertex Types
   *  - Build Edge Types
   *
   *  @param nameResolver
   *  @param context
   */

  public void buildGraphModel(ONameResolver nameResolver, OTeleporterContext context) {

    OTeleporterStatistics statistics = context.getStatistics();
    statistics.startWork2Time = new Date();
    statistics.runningStepNumber = 2;


    /*
     *  Vertex-types building
     */

    this.buildVertexTypes(nameResolver, context);


    /*
     *  Edge-types building
     */

    this.buildEdgeTypes(nameResolver, context);

    statistics.notifyListeners();
    statistics.runningStepNumber = -1;
  }


  /**
   * MICRO EXECUTION BLOCK: BUILD GRAPH MODEL - BUILD VERTEX TYPES
   * Builds the Vertex Types starting from the Entities in the source database schema.
   *
   * @param nameResolver
   * @param context
   */

  private void buildVertexTypes(ONameResolver nameResolver, OTeleporterContext context) {

    OTeleporterStatistics statistics = context.getStatistics();

    int numberOfVertexType = this.dataBaseSchema.getEntities().size();
    statistics.totalNumberOfModelVertices = numberOfVertexType;
    int iteration = 1;
    for(OEntity currentEntity: this.dataBaseSchema.getEntities()) {

      context.getOutputManager().debug("\nBuilding '%s' vertex-type (%s/%s)...\n", currentEntity.getName(), iteration, numberOfVertexType);

      // building correspondent vertex-type
      String currentVertexTypeName = nameResolver.resolveVertexName(currentEntity.getName());

      // fetch the vertex type from the graph model (empty vertex, only name defined), if does not exist create it.
      boolean alreadyPresentInGraphModel = true;
      OVertexType currentVertexType = this.graphModel.getVertexByName(currentVertexTypeName);
      if(currentVertexType == null) {
        currentVertexType = new OVertexType(currentVertexTypeName);
        alreadyPresentInGraphModel = false;
      }

      // recognizing joint tables of dimension 2
      if(currentEntity.isAggregableJoinTable())
        currentVertexType.setIsFromJoinTable(true);
      else
        currentVertexType.setIsFromJoinTable(false);

      // adding attributes to vertex-type
      for(OAttribute attribute: currentEntity.getAttributes()) {
        OModelProperty currentProperty = new OModelProperty(nameResolver.resolveVertexProperty(attribute.getName()), attribute.getOrdinalPosition(), attribute.getDataType(), currentEntity.getPrimaryKey().getInvolvedAttributes().contains(attribute));
        currentVertexType.getProperties().add(currentProperty);
      }

      // adding parent vertex if the corresponding entity has a parent
      if(currentEntity.getParentEntity() != null) {
        OElementType currentParentElement = this.graphModel.getVertexByNameIgnoreCase(currentEntity.getParentEntity().getName());
        currentVertexType.setParentType(currentParentElement);
        currentVertexType.setInheritanceLevel(currentEntity.getInheritanceLevel());
      }

      // adding inherited attributes to vertex-type
      for(OAttribute attribute: currentEntity.getInheritedAttributes()) {
        OModelProperty currentProperty = new OModelProperty(nameResolver.resolveVertexProperty(attribute.getName()), attribute.getOrdinalPosition(), attribute.getDataType(), currentEntity.getPrimaryKey().getInvolvedAttributes().contains(attribute));
        currentVertexType.getInheritedProperties().add(currentProperty);
      }

      // adding vertex to the graph model
      if(!alreadyPresentInGraphModel) {
        this.graphModel.getVerticesType().add(currentVertexType);
      }

      // rules updating
      this.upsertEntityVertexRules(currentVertexType, currentEntity);

      iteration++;
      context.getOutputManager().debug("\nVertex-type %s built.\n", currentVertexTypeName);
      statistics.builtModelVertexTypes++;
    }

    // sorting vertices type for inheritance level and then for name
    Collections.sort(this.graphModel.getVerticesType());
  }


  /**
   * MICRO EXECUTION BLOCK: BUILD GRAPH MODEL - BUILD EDGE TYPES
   * Builds the Edge Types starting from the Relationships in the source database schema.
   *
   * @param nameResolver
   * @param context
   */

  private void buildEdgeTypes(ONameResolver nameResolver, OTeleporterContext context) {

    OTeleporterStatistics statistics = context.getStatistics();

    int numberOfEdgeType = this.dataBaseSchema.getRelationships().size();
    statistics.totalNumberOfEdgeType = numberOfEdgeType;
    String edgeType = null;
    int iteration = 1;

    if (numberOfEdgeType > 0) {

      // edges added through relationships (foreign keys of db)
      for(OEntity currentEntity: this.dataBaseSchema.getEntities()) {

        for(ORelationship relationship: currentEntity.getOutRelationships()) {

          OVertexType currentOutVertex = this.graphModel.getVertexByName(nameResolver.resolveVertexName(relationship.getForeignEntityName()));
          OVertexType currentInVertex = this.graphModel.getVertexByName(nameResolver.resolveVertexName(relationship.getParentEntityName()));
          context.getOutputManager().debug("\nBuilding edge-type from '%s' to '%s' (%s/%s)...\n", currentOutVertex.getName(), currentInVertex.getName(), iteration, numberOfEdgeType);

          if(currentOutVertex != null && currentInVertex != null) {

            // check on the presence of the relationship in the map performed in order to avoid generating several edgeTypes for the same relationship.
            // when the edge was built before from the configuration and the relationship was inserted with that edgeType in the map, the relationships
            // mustn't be analyzed at this point! CHANGE IT when you'll implement the pipeline
            if(!this.relationship2edgeType.containsKey(relationship)) {

              // relationships which represents inheritance between different entities don't generate new edge-types,
              // thus new edge type is created iff the parent-table's name (of the relationship) does not coincide
              // with the name of the parent entity of the current entity.
              if (currentEntity.getParentEntity() == null || !currentEntity.getParentEntity().getName().equals(relationship.getParentEntityName())) {

                // if the class edge doesn't exists, it will be created
                edgeType = nameResolver.resolveEdgeName(relationship);

                OEdgeType currentEdgeType = this.graphModel.getEdgeTypeByName(edgeType);
                if (currentEdgeType == null) {
                  currentEdgeType = new OEdgeType(edgeType, null, currentInVertex);
                  this.graphModel.getEdgesType().add(currentEdgeType);
                  context.getOutputManager().debug("\nEdge-type %s built.\n", currentEdgeType.getName());
                  statistics.builtModelEdgeTypes++;
                } else {
                  // edge already present, the counter of relationships represented by the edge is incremented
                  currentEdgeType.setNumberRelationshipsRepresented(currentEdgeType.getNumberRelationshipsRepresented() + 1);
                }

                // adding the edge to the two vertices
                if (!currentOutVertex.getOutEdgesType().contains(currentEdgeType)) {
                  currentOutVertex.getOutEdgesType().add(currentEdgeType);
                }
                if (!currentInVertex.getInEdgesType().contains(currentEdgeType)) {
                  currentInVertex.getInEdgesType().add(currentEdgeType);
                }

                // rules updating
                this.upsertRelationshipEdgeRules(relationship, currentEdgeType);
              }
            }
          }
          else {
            context.getOutputManager().error("Error during graph model building phase: information loss, relationship missed. Edge-type not built.\n");
          }

          iteration++;
        }

        for(ORelationship relationship: currentEntity.getInheritedOutRelationships()) {
          OVertexType currentOutVertex = this.graphModel.getVertexByName(nameResolver.resolveVertexName(currentEntity.getName()));
          OVertexType currentInVertex = this.graphModel.getVertexByName(nameResolver.resolveVertexName(relationship.getParentEntityName()));
          context.getOutputManager().debug("\nBuilding edge-type from '%s' to '%s' (%s/%s)...\n", currentOutVertex.getName(), currentInVertex.getName(), iteration, numberOfEdgeType);

          if(currentOutVertex != null && currentInVertex != null) {

            OEdgeType currentEdgeType = this.graphModel.getEdgeTypeByName(edgeType);

            // adding the edge to the two vertices
            currentOutVertex.getOutEdgesType().add(currentEdgeType);
            currentInVertex.getInEdgesType().add(currentEdgeType);
            context.getOutputManager().debug("\nEdge-type built.\n");
          }
          else {
            context.getOutputManager().error("Error during graph model building phase: information loss, relationship missed. Edge-type not built.\n");
          }
        }
      }
    }
  }

  /**
   * MACRO EXECUTION BLOCK: APPLY IMPORT CONFIGURATION
   * Builds the graph model and the rules for the mapping with the source database schema through 2 micro execution blocks:
   *  - upsert Relationships according to the configuration
   *
   *  @param context
   */

  public void applyImportConfiguration(OTeleporterContext context) {

    /*
     * Adding/updating relationships according to the manual configuration
     */

    if(this.configuration != null) {
      this.upsertRelationshipsFromConfiguration(context);
    }
  }


  /**
   * MICRO EXECUTION BLOCK: APPLY IMPORT CONFIGURATION - UPSERT RELATIONSHIPS FROM CONFIGURATION
   * Builds the Edge Types starting from the Relationships in the source database schema.
   * Adds and/or updates Relationships in the source database schema according to the manual configuration passed to Teleporter.
   *
   * @param context
   */

  private void upsertRelationshipsFromConfiguration(OTeleporterContext context) {

    List<ODocument> edgesDoc = configuration.field("edges");

    if(edgesDoc == null) {
      context.getOutputManager().error("Configuration error: 'edges' field not found.");
      throw new OTeleporterRuntimeException();
    }

    // building the current-in-vertex and the current-out-vertex and adding the edge to them
    ONameResolver nameResolver = context.getNameResolver();

    for(ODocument currentEdge: edgesDoc) {

      String[] edgeFields = currentEdge.fieldNames();
      if(edgeFields.length != 1) {

      }
      String edgeName = edgeFields[0];
      ODocument currentEdgeInfo = currentEdge.field(edgeName);
      ODocument mappingDoc = currentEdgeInfo.field("mapping");

      // building relationship
      if(mappingDoc != null) {

        String currentForeignEntityName = mappingDoc.field("fromTable");
        String currentParentEntityName = mappingDoc.field("toTable");
        List<String> fromColumns = mappingDoc.field("fromColumns");
        List<String> toColumns = mappingDoc.field("toColumns");
        ODocument joinTableDoc = mappingDoc.field("joinTable");

        // configuration errors managing (draconian approach)
        if(currentForeignEntityName == null) {
          context.getOutputManager().error("Configuration error: 'fromTable' field not found in the '%s' edge-type definition.",  edgeName);
          throw new OTeleporterRuntimeException();
        }
        if(currentParentEntityName == null) {
          context.getOutputManager().error("Configuration error: 'toTable' field not found in the '%s' edge-type definition.",  edgeName);
          throw new OTeleporterRuntimeException();
        }
        if(fromColumns == null) {
          context.getOutputManager().error("Configuration error: 'fromColumns' field not found in the '%s' edge-type definition.",  edgeName);
          throw new OTeleporterRuntimeException();
        }
        if(toColumns == null) {
          context.getOutputManager().error("Configuration error: 'toColumns' field not found in the '%s' edge-type definition.",  edgeName);
          throw new OTeleporterRuntimeException();
        }

        String direction = mappingDoc.field("direction");

        if(direction != null && !(direction.equals("direct") || direction.equals("inverse"))) {
          context.getOutputManager().error("Configuration error: direction for the edge %s cannot be '%s'. Allowed values: 'direct' or 'inverse' \n", edgeName, direction);
          throw new OTeleporterRuntimeException();
        }

        boolean foreignEntityIsJoinTableToAggregate = false;

        if(joinTableDoc == null) {

          // building relationship
          ORelationship currentRelationship = buildRelationshipFrom(currentForeignEntityName, currentParentEntityName, fromColumns, toColumns, direction, context);

          // building correspondent edgeType (check on inheritance not needed)
          buildEdgeTypeFromRelationship(currentRelationship, currentForeignEntityName, currentParentEntityName, edgeName, currentEdgeInfo, foreignEntityIsJoinTableToAggregate, context);

        }
        else {

          String joinTableName = joinTableDoc.field("tableName");

          if(joinTableName == null) {
            context.getOutputManager().error("Configuration error: 'tableName' field not found in the join table mapping with the '%s' edge-type.",  edgeName);
            throw new OTeleporterRuntimeException();
          }

          foreignEntityIsJoinTableToAggregate = true;

          if(context.getExecutionStrategy().equals("naive-aggregate")) { // strategy is aggregated
            List<String> joinTableFromColumns = joinTableDoc.field("fromColumns");
            List<String> joinTableToColumns = joinTableDoc.field("toColumns");

            if(joinTableFromColumns == null) {
              context.getOutputManager().error("Configuration error: 'fromColumns' field not found in the join table mapping with the '%s' edge-type.",  edgeName);
              throw new OTeleporterRuntimeException();
            }
            if(joinTableToColumns == null) {
              context.getOutputManager().error("Configuration error: 'toColumns' field not found in the join table mapping with the '%s' edge-type.",  edgeName);
              throw new OTeleporterRuntimeException();
            }

            // building left relationship
            ORelationship currentRelationship = buildRelationshipFrom(joinTableName, currentForeignEntityName, joinTableFromColumns, fromColumns, direction, context);

            // building correspondent edgeType (check on inheritance not needed)
            buildEdgeTypeFromRelationship(currentRelationship, joinTableName, currentForeignEntityName, edgeName + "-left", currentEdgeInfo, foreignEntityIsJoinTableToAggregate, context);

            // building right relationship
            currentRelationship = buildRelationshipFrom(joinTableName, currentParentEntityName, joinTableToColumns, toColumns, direction, context);
            // building correspondent edgeType (check on inheritance not needed)
            buildEdgeTypeFromRelationship(currentRelationship, joinTableName, currentParentEntityName, edgeName + "-right", currentEdgeInfo, foreignEntityIsJoinTableToAggregate, context);

            // setting attributes of the join table
            OEntity joinTable = this.dataBaseSchema.getEntityByName(joinTableName);
            joinTable.setIsAggregableJoinTable(true);
            joinTable.setDirectionOfN2NRepresentedRelationship(direction);
            joinTable.setNameOfN2NRepresentedRelationship(edgeName);

            // setting attributes of the correspondent vertex type
            OVertexType correspondentVertexType = this.entity2vertexType.get(joinTable);
            correspondentVertexType.setIsFromJoinTable(true);

          }
          else if(context.getExecutionStrategy().equals("naive")) {
            context.getOutputManager().error("Configuration not compliant with the chosen strategy: you cannot perform the aggregation declared in the configuration for the "
                    + "join table %s while executing migration with a not-aggregating strategy. Thus no aggregation will be performed.\n", joinTableName);
            throw new OTeleporterRuntimeException();
          }

        }
      }
      else {
        context.getOutputManager().error("Configuration error: 'mapping' field not found in the '%s' edge-type definition.",  edgeName);
        throw new OTeleporterRuntimeException();
      }
    }

  }


  /**
   *
   * @param currentForeignEntityName
   * @param currentParentEntityName
   * @param fromColumns
   * @param toColumns
   * @param direction
   * @param context
   * @return
   */
  private ORelationship buildRelationshipFrom(String currentForeignEntityName, String currentParentEntityName, List<String> fromColumns,
                                              List<String> toColumns, String direction, OTeleporterContext context) {

    OTeleporterStatistics statistics = context.getStatistics();

    // fetching foreign and parent entities
    OEntity currentForeignEntity = this.dataBaseSchema.getEntityByName(currentForeignEntityName);
    OEntity currentParentEntity = this.dataBaseSchema.getEntityByName(currentParentEntityName);

    // fetch relationship from current db schema, if not present create a new one
    boolean relationshipAlreadyPresentInDBSchema = true;
    ORelationship currentRelationship = this.dataBaseSchema.getRelationshipByInvolvedEntitiesAndAttributes(currentForeignEntityName, currentParentEntityName, fromColumns, toColumns);
    if (currentRelationship == null) {
      currentRelationship = new ORelationship(currentForeignEntityName, currentParentEntityName);
      relationshipAlreadyPresentInDBSchema = false;

      // updating statistics
      statistics.detectedRelationships += 1;
      statistics.totalNumberOfRelationships += 1;
    }

    // adding the direction of the relationship if different from null
    if (direction != null) {
      if ((direction.equals("direct") || direction.equals("inverse"))) {
        currentRelationship.setDirection(direction);
      } else {
        context.getOutputManager().error(
                "Wrong value for the direction of the relationship between %s and %s: \"%s\" is not a valid direction. " + "Please choose between \"direct\" or \"inverse\" \n", currentRelationship.getForeignEntityName(),
                currentRelationship.getParentEntityName(), direction);
      }
    }

    // if Relationship was not already present in the schema we must add the foreign key both to the 'foreign entity' and the relationship, and the primary key to the relationship in the db schema.
    if (!relationshipAlreadyPresentInDBSchema) {
      OForeignKey currentFk = new OForeignKey(currentForeignEntity);

      // adding attributes involved in the foreign key
      for (String column : fromColumns) {
        currentFk.addAttribute(currentForeignEntity.getAttributeByName(column));
      }

      // searching correspondent primary key
      OPrimaryKey currentPk = this.dataBaseSchema.getEntityByName(currentParentEntityName).getPrimaryKey();
      currentRelationship.setPrimaryKey(currentPk);
      currentRelationship.setForeignKey(currentFk);

      currentForeignEntity.getForeignKeys().add(currentFk);
      this.dataBaseSchema.getRelationships().add(currentRelationship);

      // adding relationship to the current entity
      currentForeignEntity.getOutRelationships().add(currentRelationship);
      currentParentEntity.getInRelationships().add(currentRelationship);
    }


    return currentRelationship;
  }


  /**
   *
   * @param currentRelationship
   * @param currentForeignEntityName
   * @param currentParentEntityName
   * @param edgeName
   * @param currentEdgeInfo
   * @param foreignEntityIsJoinTableToAggregate
   * @param context
   */
  private void buildEdgeTypeFromRelationship(ORelationship currentRelationship, String currentForeignEntityName, String currentParentEntityName,
                                             String edgeName, ODocument currentEdgeInfo, boolean foreignEntityIsJoinTableToAggregate, OTeleporterContext context) {

    OTeleporterStatistics statistics = context.getStatistics();
    ONameResolver nameResolver = context.getNameResolver();

    // retrieving edge type, if not present is created from scratch
    boolean edgeTypeAlreadyPresent = true;
    OEdgeType currentEdgeType = this.relationship2edgeType.get(currentRelationship);
    if(currentEdgeType == null) {
      edgeTypeAlreadyPresent = false;
      currentEdgeType = new OEdgeType(edgeName, null, null);
      this.graphModel.getEdgesType().add(currentEdgeType);
      context.getOutputManager().debug("\nEdge-type %s built.\n", currentEdgeType.getName());
      statistics.builtModelEdgeTypes++;
    }
    currentEdgeType.setName(edgeName);
//    else {
//      // edge already present, the counter of relationships represented by the edge is incremented
//      currentEdgeType.setNumberRelationshipsRepresented(currentEdgeType.getNumberRelationshipsRepresented() +1);
//    }

    // extracting properties info if present and adding them to the current edge-type
    List<OModelProperty> properties = new LinkedList<OModelProperty>();
    ODocument edgePropsDoc = currentEdgeInfo.field("properties");
    // adding properties to the edge
    if(edgePropsDoc != null) {
      String[] propertiesFields = edgePropsDoc.fieldNames();

      int ordinalPosition = currentEdgeType.getProperties().size() + 1;
      for(String propertyName: propertiesFields) {
        ODocument currentEdgePropertyDoc = edgePropsDoc.field(propertyName);
        String propertyType = currentEdgePropertyDoc.field("type");
        if(propertyType == null) {
          context.getStatistics().warningMessages.add("Configuration ERROR: the property " + propertyName + " will not added to the Edge-Type " + currentEdgeType.getName() + " because the type is badly defined or not defined at all.");
          continue;
        }
        OModelProperty currentProperty = currentEdgeType.getPropertyByName(propertyName);
        if(currentProperty == null) {
          currentProperty = new OModelProperty(propertyName, ordinalPosition, propertyType, false);
          ordinalPosition++;
        }
        currentProperty.setFromPrimaryKey(false);
        Boolean mandatory = currentEdgePropertyDoc.field("mandatory");
        if(mandatory != null) {
          currentProperty.setMandatory(mandatory);
        }
        Boolean readOnly = currentEdgePropertyDoc.field("readOnly");
        if(readOnly != null) {
          currentProperty.setReadOnly(readOnly);
        }
        Boolean notNull = currentEdgePropertyDoc.field("notNull");
        if(notNull != null) {
          currentProperty.setNotNull(notNull);
        }
        currentEdgeType.getProperties().add(currentProperty);
      }

    }

    String currentRelationshipDirection = currentRelationship.getDirection();
    OVertexType currentInVertexType;
    OVertexType currentOutVertexType;

    if(edgeTypeAlreadyPresent) {

      // if the edge type was already present and the direction is inverse (as to say if it was modified) the references with in-vertex and out-vertex must to be updated
      if(currentRelationshipDirection != null && currentRelationshipDirection.equals("inverse")) {
        currentInVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentForeignEntityName));
        currentOutVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentParentEntityName));

        // removing on vertices the old references to the edge type
        currentInVertexType.getOutEdgesType().remove(currentEdgeType);
        currentOutVertexType.getInEdgesType().remove(currentEdgeType);

        // defining on vertices the new references to the edge type
        currentInVertexType.getInEdgesType().add(currentEdgeType);
        currentOutVertexType.getOutEdgesType().add(currentEdgeType);
        currentEdgeType.setInVertexType(currentInVertexType);
      }
    }

    // new references with in-vertex and out-vertex are added and the old
    else {

      /**
       *  Direction of the edge is decided according to three conditions:
       *  - direction != null or direction == null
       *  - current foreign entity is a join table or not
       *  - direction is direct or inverse
       */

      // if direction is null the edge will be direct by default
      if (currentRelationshipDirection == null) {
        currentInVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentParentEntityName));
        currentOutVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentForeignEntityName));
      } else {

        // if the current foreign entity is a join table we will aggregate then the edge will be direct
        if (foreignEntityIsJoinTableToAggregate) {
          currentInVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentParentEntityName));
          currentOutVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentForeignEntityName));
        } else {

          // edge direction chosen according to the value of the parameter direction
          if (currentRelationshipDirection.equals("direct")) {
            currentInVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentParentEntityName));
            currentOutVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentForeignEntityName));
          } else {
            currentInVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentForeignEntityName));
            currentOutVertexType = this.loadOrCreateVertexType(nameResolver.resolveVertexName(currentParentEntityName));
          }
        }
      }

      currentInVertexType.getInEdgesType().add(currentEdgeType);
      currentOutVertexType.getOutEdgesType().add(currentEdgeType);
      currentEdgeType.setInVertexType(currentInVertexType);

      // rules updating
      upsertRelationshipEdgeRules(currentRelationship, currentEdgeType);
    }

  }


  // It loads the vertex type by name. If it is not present it will be created and added to the graph model.
  private OVertexType loadOrCreateVertexType(String vertexTypeName) {

    OVertexType vertexType = this.graphModel.getVertexByName(vertexTypeName);
    if(vertexType == null) {
      vertexType = new OVertexType(vertexTypeName);
      this.graphModel.getVerticesType().add(vertexType);
    }

    return vertexType;
  }


  /**
   * MACRO EXECUTION BLOCK: PERFORM AGGREGATIONS
   * Performs aggregation strategies on the graph model through the following micro execution blocks:
   *  - Many-To-Many Aggregation
   *
   * @param context
   */

  public void performAggregations(OTeleporterContext context) {

    /*
     * Many-To-Many Aggregation
     */
    performMany2ManyAggregation(context);
  }


  /**
   * MICRO EXECUTION BLOCK: PERFORM AGGREGATIONS - MANY TO MANY AGGREGATION
   * Aggregates Many-To-Many Relationships represented by join tables of dimension == 2.
   *
   * @param context
   */

  public void performMany2ManyAggregation(OTeleporterContext context) {

    OTeleporterStatistics statistics = context.getStatistics();
    Iterator<OVertexType> it = this.graphModel.getVerticesType().iterator();

    context.getOutputManager().debug("\n\nJoin Table aggregation phase...\n");

    while(it.hasNext()) {
      OVertexType currentVertexType = it.next();

      // if vertex is obtained from a join table of dimension 2,
      // then aggregation is performed
      if(currentVertexType.isFromJoinTable() && currentVertexType.getOutEdgesType().size() == 2) {
        statistics.totalNumberOfModelVertices--;

        // building new edge
        OEdgeType currentOutEdge1 = currentVertexType.getOutEdgesType().get(0);
        OEdgeType currentOutEdge2 = currentVertexType.getOutEdgesType().get(1);

        OVertexType outVertexType;
        OVertexType inVertexType;
        String direction = this.vertexType2entity.get(currentVertexType).getDirectionOfN2NRepresentedRelationship();
        if(direction.equals("direct")) {
          outVertexType = currentOutEdge1.getInVertexType();
          inVertexType = currentOutEdge2.getInVertexType();
        }
        else {
          outVertexType = currentOutEdge2.getInVertexType();
          inVertexType = currentOutEdge1.getInVertexType();
        }

        OEntity joinTable = this.vertexType2entity.get(currentVertexType);
        String nameOfRelationship = joinTable.getNameOfN2NRepresentedRelationship();
        String edgeType;
        if(nameOfRelationship != null)
          edgeType = nameOfRelationship;
        else
          edgeType = currentVertexType.getName();

        OEdgeType newAggregatorEdge = new OEdgeType(edgeType, null, inVertexType);

        int position = 1;
        // adding to the edge all properties not belonging to the primary key
        for(OModelProperty currentProperty: currentVertexType.getProperties()) {

          // if property does not belong to the primary key
          if(!currentProperty.isFromPrimaryKey()) {
            OModelProperty newProperty = new OModelProperty(currentProperty.getName(), position, currentProperty.getPropertyType(), currentProperty.isFromPrimaryKey());
            if(currentProperty.isMandatory() != null)
              newProperty.setMandatory(currentProperty.isMandatory());
            if(currentProperty.isReadOnly() != null)
              newProperty.setReadOnly(currentProperty.isReadOnly());
            if(currentProperty.isNotNull() != null)
              newProperty.setNotNull(currentProperty.isNotNull());
            newAggregatorEdge.getProperties().add(newProperty);
            position++;
          }
        }

        // adding to the edge all properties belonging to the old edges
        for(OModelProperty currentProperty: currentOutEdge1.getProperties()) {
          if(newAggregatorEdge.getPropertyByName(currentProperty.getName()) == null) {
            OModelProperty newProperty = new OModelProperty(currentProperty.getName(), position, currentProperty.getPropertyType(), currentProperty.isFromPrimaryKey());
            if(currentProperty.isMandatory() != null)
              newProperty.setMandatory(currentProperty.isMandatory());
            if(currentProperty.isReadOnly() != null)
              newProperty.setReadOnly(currentProperty.isReadOnly());
            if(currentProperty.isNotNull() != null)
              newProperty.setNotNull(currentProperty.isNotNull());
            newAggregatorEdge.getProperties().add(newProperty);
            position++;
          }
        }
        for(OModelProperty currentProperty: currentOutEdge2.getProperties()) {
          if(newAggregatorEdge.getPropertyByName(currentProperty.getName()) == null) {
            OModelProperty newProperty = new OModelProperty(currentProperty.getName(), position, currentProperty.getPropertyType(), currentProperty.isFromPrimaryKey());
            if(currentProperty.isMandatory() != null)
              newProperty.setMandatory(currentProperty.isMandatory());
            if(currentProperty.isReadOnly() != null)
              newProperty.setReadOnly(currentProperty.isReadOnly());
            if(currentProperty.isNotNull() != null)
              newProperty.setNotNull(currentProperty.isNotNull());
            newAggregatorEdge.getProperties().add(newProperty);
            position++;
          }
        }

        // removing old edges from graph model and from vertices' "in-edges" collection
        currentOutEdge1.setNumberRelationshipsRepresented(currentOutEdge1.getNumberRelationshipsRepresented()-1);
        currentOutEdge2.setNumberRelationshipsRepresented(currentOutEdge2.getNumberRelationshipsRepresented()-1);

        if(currentOutEdge1.getNumberRelationshipsRepresented() == 0) {
          this.graphModel.getEdgesType().remove(currentOutEdge1);
          statistics.builtModelEdgeTypes--;
        }
        if(currentOutEdge2.getNumberRelationshipsRepresented() == 0) {
          this.graphModel.getEdgesType().remove(currentOutEdge2);
          statistics.builtModelEdgeTypes--;
        }
        if(direction.equals("direct")) {
          outVertexType.getInEdgesType().remove(currentOutEdge1);
          inVertexType.getInEdgesType().remove(currentOutEdge2);
        }
        else {
          outVertexType.getInEdgesType().remove(currentOutEdge2);
          inVertexType.getInEdgesType().remove(currentOutEdge1);
        }

        // adding entry to the map
        this.joinVertex2aggregatorEdges.put(currentVertexType, new OAggregatorEdge(outVertexType.getName(), inVertexType.getName(), newAggregatorEdge));

        // removing old vertex
        it.remove();
        statistics.builtModelVertexTypes--;

        // adding new edge to graph model
        this.graphModel.getEdgesType().add(newAggregatorEdge);
        statistics.builtModelEdgeTypes++;

        // adding new edge to the vertices' "in/out-edges" collections
        outVertexType.getOutEdgesType().add(newAggregatorEdge);
        inVertexType.getInEdgesType().add(newAggregatorEdge);
      }
    }
    context.getOutputManager().debug("\nAggregation performed.\n");
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


  public OEntity getEntityByVertexType(String vertexType) {

    if(vertexType != null) {

      for(OEntity currentEntity: this.entity2vertexType.keySet()) {
        if(this.entity2vertexType.get(currentEntity).getName().equals(vertexType)) {
          return currentEntity;
        }
      }
    }
    return null;
  }

  // TO UPDATE WITH THE INVERTED MAP vertexType2entity (right?)
  public OAttribute getAttributeByVertexTypeAndProperty(OVertexType vertexType, String propertyName) {

    int position = 0;
    OModelProperty currentProperty;

    if(vertexType != null) {

      OEntity correspondentEntity = this.vertexType2entity.get(vertexType);

      if(correspondentEntity != null) {

        currentProperty = vertexType.getPropertyByName(propertyName);

        // if the current vertex has not the current property and if it has parents, a recursive lookup is performed (inheritance case)
        if(currentProperty == null) {
          OVertexType parentType = (OVertexType) vertexType.getParentType();
          if(parentType != null) {
            return this.getAttributeByVertexTypeAndProperty(parentType, propertyName);
          }
        }
        else {
          position = currentProperty.getOrdinalPosition();
          return correspondentEntity.getAttributeByOrdinalPosition(position);
        }
      }
    }
    return null;
  }


  /**
   * It returns the vertex type mapped with the aggregator edge correspondent to the original join table.
   *
   * @param edgeType
   * @return
   */

  public OVertexType getJoinVertexTypeByAggregatorEdge(String edgeType) {

    for(Map.Entry<OVertexType,OAggregatorEdge> entry: this.joinVertex2aggregatorEdges.entrySet()) {
      if(entry.getValue().getEdgeType().getName().equals(edgeType)) {
        OVertexType joinVertexType = entry.getKey();
        return joinVertexType;
      }
    }
    return null;
  }

  public OAggregatorEdge getAggregatorEdgeByJoinVertexTypeName(String vertexTypeName) {

    for(OVertexType currentVertexType: this.joinVertex2aggregatorEdges.keySet()) {
      if(currentVertexType.getName().equals(vertexTypeName)) {
        return this.joinVertex2aggregatorEdges.get(currentVertexType);
      }
    }
    return null;
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

  public Map<OVertexType, OAggregatorEdge> getJoinVertex2aggregatorEdges() {
    return joinVertex2aggregatorEdges;
  }


  public void setJoinVertex2aggregatorEdges(Map<OVertexType, OAggregatorEdge> joinVertex2aggregatorEdges) {
    this.joinVertex2aggregatorEdges = joinVertex2aggregatorEdges;
  }


  public List<String> getIncludedTables() {
    return includedTables;
  }


  public void setIncludedTables(List<String> includedTables) {
    this.includedTables = includedTables;
  }


  public List<String> getExcludedTables() {
    return excludedTables;
  }


  public void setExcludedTables(List<String> excludedTables) {
    this.excludedTables = excludedTables;
  }

  public ODocument getConfiguration() {
    return this.configuration;
  }

  public void setConfiguration(ODocument configuration) {
    this.configuration = configuration;
  }

  public boolean isTableAllowed(String tableName) {

    if(this.includedTables.size() > 0)
      return this.includedTables.contains(tableName);
    else if (this.excludedTables.size() > 0)
      return !this.excludedTables.contains(tableName);
    else
      return true;

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
