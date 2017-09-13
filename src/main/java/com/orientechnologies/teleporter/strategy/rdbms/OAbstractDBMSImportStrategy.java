/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.teleporter.strategy.rdbms;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.configuration.api.OConfiguredVertexClass;
import com.orientechnologies.teleporter.configuration.api.OSourceTable;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.factory.ODataTypeHandlerFactory;
import com.orientechnologies.teleporter.factory.ONameResolverFactory;
import com.orientechnologies.teleporter.importengine.rdbms.dbengine.ODBQueryEngine;
import com.orientechnologies.teleporter.importengine.rdbms.graphengine.OGraphEngineForDB;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OEEClassMapper;
import com.orientechnologies.teleporter.model.OSourceInfo;
import com.orientechnologies.teleporter.model.dbschema.*;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OModelProperty;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientechnologies.teleporter.persistence.util.OQueryResult;
import com.orientechnologies.teleporter.strategy.OWorkflowStrategy;
import com.orientechnologies.teleporter.util.OFunctionsHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public abstract class OAbstractDBMSImportStrategy implements OWorkflowStrategy {

  protected OER2GraphMapper mapper;
  protected String protocol;
  protected String serverInitUrl;
  protected String dbName;

  public OAbstractDBMSImportStrategy(String protocol, String serverInitUrl, String dbName) {
    this.protocol = protocol;
    this.serverInitUrl = serverInitUrl;
    this.dbName = dbName;
  }

  @Override
  public ODocument executeStrategy(OSourceInfo sourceInfo, String outOrientGraphUri, String chosenMapper, String xmlPath,
      String nameResolverConvention, List<String> includedTables, List<String> excludedTables, ODocument migrationConfigDoc) {

    OSourceDatabaseInfo sourceDBInfo = (OSourceDatabaseInfo) sourceInfo;

    Date globalStart = new Date();

    ODataTypeHandlerFactory dataTypeHandlerFactory = new ODataTypeHandlerFactory();
    ODBMSDataTypeHandler handler = (ODBMSDataTypeHandler) dataTypeHandlerFactory.buildDataTypeHandler(sourceDBInfo.getDriverName());
    OConfigurationHandler configurationHandler = this.buildConfigurationHandler();

    /**
     * Building configuration
     */

    boolean keepVerticesCoordinates = true;
    OConfiguration migrationConfig = null;
    if (migrationConfigDoc != null) {

      // Applying filters to starting migrationConfigDoc
      if(includedTables != null && includedTables.size() > 0) {
        configurationHandler.filterAccordingToWhiteList(migrationConfigDoc, includedTables);
      }
      else if(excludedTables != null && excludedTables.size() > 0) {
        configurationHandler.filterAccordingToBlackList(migrationConfigDoc, excludedTables);
      }

      migrationConfig = configurationHandler.buildConfigurationFromJSONDoc(migrationConfigDoc, keepVerticesCoordinates);
    }


    /*
     * Step 1,2,3
     */

    ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
    ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention);
    OTeleporterContext.getInstance().getStatistics().runningStepNumber = -1;

    this.mapper = this.createSchemaMapper(sourceDBInfo, chosenMapper, xmlPath, nameResolver, handler, includedTables, excludedTables, migrationConfig);


    /*
     * Step 4: Import
     */

    this.executeImport(sourceDBInfo, dbName, mapper, handler);
    OTeleporterContext.getInstance().getStatistics().notifyListeners();
    OTeleporterContext.getInstance().getMessageHandler().info("\n");
    OTeleporterContext.getInstance().getStatistics().runningStepNumber = -1;

    Date globalEnd = new Date();

    OTeleporterContext.getInstance().getMessageHandler()
        .info("\n\nImporting complete in %s\n", OFunctionsHandler.getHMSFormat(globalStart, globalEnd));
    OTeleporterContext.getInstance().getMessageHandler().info(OTeleporterContext.getInstance().getStatistics().toString());

    // Building Graph Model mapping (for graph rendering too)
    // It must remain until the config will follow a delta definition approach, and not a full definition approach
    OConfiguration configuredGraph = configurationHandler.buildConfigurationFromMapper(this.mapper);
    ODocument configuredGraphDoc = configurationHandler.buildJSONDocFromConfiguration(configuredGraph);

    return configuredGraphDoc;
  }

  protected abstract OConfigurationHandler buildConfigurationHandler();

  public abstract OER2GraphMapper createSchemaMapper(OSourceDatabaseInfo sourceDBInfo, String chosenMapper, String xmlPath, ONameResolver nameResolver, ODBMSDataTypeHandler handler,
      List<String> includedTables, List<String> excludedTables, OConfiguration migrationConfig);

  public abstract void executeImport(OSourceDatabaseInfo sourceDBInfo, String outOrientGraphUri, OSource2GraphMapper mapper,
      ODBMSDataTypeHandler handler);

  /**
   * It imports all vertices into a Vertex Class (so 1 or more mapped entities). It's used to import all the vertices and the edges
   * belonging to an Edge Class coming from a Canonical Relationship in the source database.
   *
   * @param mappedEntities
   * @param aggregationColumns
   * @param currentOutVertexType
   * @param dbQueryEngine
   * @param graphEngine
   * @param orientGraph
   */

  protected void importRecordsFromEntitiesIntoVertexClass(List<OEntity> mappedEntities, String[][] aggregationColumns,
      OVertexType currentOutVertexType, ODBQueryEngine dbQueryEngine, OGraphEngineForDB graphEngine, ODatabaseDocument orientGraph)
      throws SQLException {

    OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();
    OQueryResult queryResult;
    ResultSet records;

    OEdgeType edgeType;// for each entity in dbSchema all records are retrieved
    int numberOfAggregatedClasses = mappedEntities.size();
    if (numberOfAggregatedClasses == 1) {
      queryResult = dbQueryEngine.getRecordsByEntity(mappedEntities.get(0));
    } else {
      queryResult = dbQueryEngine.getRecordsFromMultipleEntities(mappedEntities, aggregationColumns);
    }

    //if (handler.geospatialImplemented && super.hasGeospatialAttributes(entity, handler)) {
    //  String query = handler.buildGeospatialQuery(entity);
    //  queryResult = dbQueryEngine.executeQuery(query);
    //}

    records = queryResult.getResult();
    ResultSet currentRecord = null;

    // each record is imported as vertex in the orient graph
    while (records.next()) {

      // upsert of the vertex
      currentRecord = records;
      OVertex currentOutVertex = graphEngine.upsertVisitedVertex(orientGraph, currentRecord, currentOutVertexType, currentOutVertexType.getExternalKey());

      // navigating relationships outgoing from the current mapped entities and for each of them all the correspondent edges are built
      // and all the in-vertices are upserted in the graph database
      this.navigateRelationshipsAndInsertReachableVertices(orientGraph, graphEngine, mappedEntities, currentRecord,
          currentOutVertexType, currentOutVertex);

      // Statistics updated
      statistics.analyzedRecords += 1 * numberOfAggregatedClasses;
    }

    // closing resultset, connection and statement
    queryResult.closeAll();

    // setting the vertex type as 'analyzed'
    currentOutVertexType.setAnalyzedInLastMigration(true);
  }

  /**
   * It navigates all the relationships outgoing from the mapped entities and for each of them it builds all the correspondent edges
   * and all the in-vertices are upserted in the graph database
   *
   * @param orientGraph
   * @param graphEngine
   * @param mappedEntities
   * @param currentRecord
   * @param outVertex
   *
   * @throws SQLException
   */
  private void navigateRelationshipsAndInsertReachableVertices(ODatabaseDocument orientGraph, OGraphEngineForDB graphEngine,
      List<OEntity> mappedEntities, ResultSet currentRecord, OVertexType outVertexType, OVertex outVertex)
      throws SQLException {

    // for each attribute of the entity belonging to a foreign key, correspondent relationship is
    // built as edge and for the referenced record a new clean vertex is built (only id will be set)
    for (OEntity entity : mappedEntities) {
      for (OCanonicalRelationship currentRelationship : entity.getOutCanonicalRelationships()) {
        OEntity currentParentEntity = mapper.getDataBaseSchema().getEntityByName(currentRelationship.getParentEntity().getName());
        OVertexType currentInVertexType = mapper.getVertexTypeByEntityAndRelationship(currentParentEntity, currentRelationship);
        OEdgeType edgeType = mapper.getRelationship2edgeType().get(currentRelationship);
        graphEngine.upsertReachedVertexWithEdge(orientGraph, currentRecord, currentRelationship, outVertex, currentInVertexType,
            edgeType.getName());
      }
    }
  }

  /**
   * It imports all the records from a split entity into all the Vertex Classes mapped with it (so 1 entity mapped with several
   * vertex classes). It's used to import all the vertices and all the "splitting-edges" belonging to an Edge Class that connects
   * those vertices coming from the same record.
   *
   * @param mappedVertices
   * @param dbQueryEngine
   * @param graphEngine
   * @param orientGraph
   */

  public void importRecordsFromSplitEntityIntoVertexClasses(List<OEntity> mappedEntities, List<OVertexType> mappedVertices,
      ODBQueryEngine dbQueryEngine, OGraphEngineForDB graphEngine, ODatabaseDocument orientGraph) throws SQLException {

    OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();
    OEntity entity = mappedEntities.get(0);     // we have just a mapped entity in the splitting case
    OQueryResult queryResult;
    ResultSet records;

    queryResult = dbQueryEngine.getRecordsByEntity(entity);
    records = queryResult.getResult();
    ResultSet currentRecord = null;

    // each record is imported as many vertices in the orient graph
    while (records.next()) {

      currentRecord = records;

      // building vertices from the record
      Map<String, OVertex> className2insertedVertex = new LinkedHashMap<String, OVertex>();
      for (OVertexType currentVertexType : mappedVertices) {
        OVertex currentOutVertex = (OVertex) graphEngine
            .upsertVisitedVertex(orientGraph, currentRecord, currentVertexType, currentVertexType.getExternalKey());

        boolean navigate = false;
        for (ORelationship currentRelationship : entity.getAllOutCanonicalRelationships()) {
          OEdgeType currEdgeType = mapper.getRelationship2edgeType().get(currentRelationship);
          if (currEdgeType != null) {
            if (currentRelationship.getDirection().equals("direct") && currentVertexType.getOutEdgesType().contains(currEdgeType)) {
              navigate = true;
              break;
            } else if (currentRelationship.getDirection().equals("inverse") && currentVertexType.getInEdgesType()
                .contains(currEdgeType)) {
              navigate = true;
              break;
            }
          }
        }

        // navigating relationships outgoing from the current mapped entities and for each of them all the correspondent edges are built
        // and all the in-vertices are upserted in the graph database
        if (navigate) {
          this.navigateRelationshipsAndInsertReachableVertices(orientGraph, graphEngine, mappedEntities, currentRecord,
              currentVertexType, currentOutVertex);
        }

        className2insertedVertex.put(currentVertexType.getName(), currentOutVertex);
      }

      /*
       * Adding coherently the splitting edges between the just added vertices
       */

      List<OEEClassMapper> classMappers = ((OER2GraphMapper) this.mapper).getEEClassMappersByEntity(entity);

      // checking that: total number of edges = number of mapped vertices -1
      int numberOfEdges = classMappers.size();
      int numberOfVertices = mappedVertices.size();
      if (numberOfEdges != numberOfVertices - 1) {
        OTeleporterContext.getInstance().getMessageHandler().error(
            "There are %s edges-type and %s vertices-type detected for the split entity %s. "
                + "For a correct splitting you must have: total number of edges = number of mapped vertices -1.", numberOfEdges,
            numberOfVertices, entity.getName());
        throw new OTeleporterRuntimeException();
      }

      for (OEEClassMapper classMapper : classMappers) {
        OEdgeType currentEdgeType = classMapper.getEdgeType();
        String currentOutVertexName = currentEdgeType.getOutVertexType().getName();
        String currentInVertexName = currentEdgeType.getInVertexType().getName();
        OVertex currentOutVertex = className2insertedVertex.get(currentOutVertexName);
        OVertex currentInVertex = className2insertedVertex.get(currentInVertexName);

        Map<String, Object> properties = new LinkedHashMap<String, Object>();

        // filling properties
        for (OModelProperty currentProperty : currentEdgeType.getAllProperties()) {
          if (currentProperty.isIncludedInMigration()) {
            String currentPropertyName = currentProperty.getName();
            String currentPropertyType = OTeleporterContext.getInstance().getDataTypeHandler()
                .resolveType(currentProperty.getOriginalType().toLowerCase(Locale.ENGLISH)).toString();
            String currentOriginalType = currentProperty.getOriginalType();

            try {
              graphEngine.extractPropertiesFromRecordIntoEdge(currentRecord, properties, currentPropertyType, currentPropertyName,
                  currentOriginalType, currentEdgeType);
            } catch (Exception e) {
              String mess =
                  "Problem encountered during the extraction of the values from the records. Edge Type: " + currentEdgeType
                      .getName() + ";\tProperty: " + currentPropertyName;
              OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
              OTeleporterContext.getInstance().printExceptionStackTrace(e, "debug");
            }
          }
        }
        graphEngine.upsertEdge(orientGraph, currentOutVertex, currentInVertex, currentEdgeType.getName(), properties, "direct");
      }

      // Statistics updated
      statistics.analyzedRecords += 1;
    }

    // closing resultset, connection and statement
    queryResult.closeAll();

    // setting the vertex type as 'analyzed'
    for (OVertexType currentVertexType : mappedVertices) {
      currentVertexType.setAnalyzedInLastMigration(true);
    }

  }

  /**
   * @param dbQueryEngine
   * @param graphEngine
   * @param orientGraph
   */

  protected void importEntitiesBelongingToHierarchies(ODBQueryEngine dbQueryEngine, OGraphEngineForDB graphEngine,
      ODatabaseDocument orientGraph) {

    for (OHierarchicalBag bag : this.mapper.getDataBaseSchema().getHierarchicalBags()) {

      switch (bag.getInheritancePattern()) {

      case "table-per-hierarchy":
        this.tablePerHierarchyImport(bag, this.mapper, dbQueryEngine, graphEngine, orientGraph);
        break;

      case "table-per-type":
        this.tablePerTypeImport(bag, this.mapper, dbQueryEngine, graphEngine, orientGraph);
        break;

      case "table-per-concrete-type":
        this.tablePerConcreteTypeImport(bag, this.mapper, dbQueryEngine, graphEngine, orientGraph);
        break;

      }
    }
  }

  /**
   * Performs import of all records of the entities contained in the hierarchical bag passed as parameter.
   * Adopted in case of "Table per Hierarchy" inheritance strategy.
   *
   * @param bag
   * @param orientGraph
   */
  protected void tablePerHierarchyImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine,
      OGraphEngineForDB graphDBCommandEngine, ODatabaseDocument orientGraph) {

    try {

      OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;
      OVertexType currentInVertexType = null;
      OVertex currentOutVertex = null;
      OEdgeType edgeType = null;

      String currentDiscriminatorValue;
      Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
      OEntity physicalCurrentEntity = it.next();

      OQueryResult queryResult = null;
      ResultSet records = null;

      for (int i = bag.getDepth2entities().size() - 1; i >= 0; i--) {
        for (OEntity currentEntity : bag.getDepth2entities().get(i)) {
          currentDiscriminatorValue = bag.getEntityName2discriminatorValue().get(currentEntity.getName());

          // for each entity in dbSchema all records are retrieved
          queryResult = dbQueryEngine
              .getRecordsFromSingleTableByDiscriminatorValue(bag.getDiscriminatorColumn(), currentDiscriminatorValue,
                  physicalCurrentEntity);
          records = queryResult.getResult();
          ResultSet currentRecord = null;

          currentOutVertexType = mapper.getVertexTypeByEntity(currentEntity);

          // each record is imported as vertex in the orient graph
          while (records.next()) {

            // upsert of the vertex
            currentRecord = records;
            currentOutVertex = (OVertex) graphDBCommandEngine
                .upsertVisitedVertex(orientGraph, currentRecord, currentOutVertexType, currentOutVertexType.getExternalKey());

            // for each attribute of the entity belonging to the primary key, correspondent relationship is
            // built as edge and for the referenced record a vertex is built (only id)
            for (OCanonicalRelationship currentRelation : currentEntity.getAllOutCanonicalRelationships()) {

              currentParentEntity = mapper.getDataBaseSchema()
                  .getEntityByNameIgnoreCase(currentRelation.getParentEntity().getName());

              // checking if parent table belongs to a hierarchical bag
              if (currentParentEntity.getHierarchicalBag() == null) {
                currentInVertexType = mapper.getVertexTypeByEntity(currentRelation.getParentEntity());
              }

              // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
              else {
                String[] propertyOfKey = new String[currentRelation.getFromColumns().size()];
                String[] valueOfKey = new String[currentRelation.getFromColumns().size()];

                int index = 0;
                for (OAttribute foreignAttribute : currentRelation.getFromColumns()) {
                  propertyOfKey[index] = currentRelation.getToColumns().get(index).getName();
                  valueOfKey[index] = currentRecord.getString((foreignAttribute.getName()));
                  index++;
                }

                // search is performed only if all the values in the foreign key are different from null (the relationship is inherited and is also consistent)
                boolean ok = true;

                for (int j = 0; j < valueOfKey.length; j++) {
                  if (valueOfKey[j] == null) {
                    ok = false;
                    break;
                  }
                }
                if (ok) {
                  it = currentParentEntity.getHierarchicalBag().getDepth2entities().get(0).iterator();
                  OEntity physicalArrivalEntity = it.next();
                  String currentArrivalEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey,
                      physicalArrivalEntity, dbQueryEngine);
                  OEntity currentArrivalEntity = mapper.getDataBaseSchema().getEntityByName(currentArrivalEntityName);
                  currentInVertexType = mapper.getVertexTypeByEntity(currentArrivalEntity);
                }
              }

              // if currentInVertexType is null then there isn't a relationship between to records, thus the edge will not be added.
              if (currentInVertexType != null) {
                edgeType = mapper.getRelationship2edgeType().get(currentRelation);
                graphDBCommandEngine
                    .upsertReachedVertexWithEdge(orientGraph, currentRecord, currentRelation, currentOutVertex, currentInVertexType,
                        edgeType.getName());
              }
            }

            // Statistics updated
            statistics.analyzedRecords++;
          }
          // closing resultset, connection and statement
          queryResult.closeAll();
        }
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      OTeleporterContext.getInstance().getMessageHandler().info("\n");

      // setting the vertex type as 'analyzed'
      currentOutVertexType.setAnalyzedInLastMigration(true);

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

  }

  /**
   * Performs import of all records of the entities contained in the hierarchical bag passed as parameter.
   * Adopted in case of "Table per Type" inheritance strategy.
   *
   * @param bag
   * @param mapper
   * @param dbQueryEngine
   * @param graphDBCommandEngine
   * @param orientGraph
   */
  protected void tablePerTypeImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine,
      OGraphEngineForDB graphDBCommandEngine, ODatabaseDocument orientGraph) {

    try {

      OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();

      ResultSet aggregateTableRecords = null;

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;
      OVertexType currentInVertexType = null;
      OVertex currentOutVertex = null;
      OEdgeType edgeType = null;
      ResultSet records;
      ResultSet currentRecord;
      ResultSet fullRecord;

      OQueryResult queryResult1 = null;
      OQueryResult queryResult2 = null;

      Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
      OEntity rootEntity = it.next();

      for (int i = bag.getDepth2entities().size() - 1; i >= 0; i--) {
        for (OEntity currentEntity : bag.getDepth2entities().get(i)) {

          // for each entity in dbSchema all records are retrieved
          queryResult1 = dbQueryEngine.getRecordsByEntity(currentEntity);
          records = queryResult1.getResult();
          currentRecord = null;

          currentOutVertexType = mapper.getVertexTypeByEntity(currentEntity);

          // each record is imported as vertex in the orient graph
          while (records.next()) {
            queryResult2 = dbQueryEngine.buildAggregateTableFromHierarchicalBag(bag);
            aggregateTableRecords = queryResult2.getResult();

            // upsert of the vertex
            currentRecord = records;

            // lookup in the aggregateTable
            String[] propertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];
            String[] valueOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];
            String[] aggregateTablePropertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];

            for (int k = 0; k < propertyOfKey.length; k++) {
              propertyOfKey[k] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(k).getName();
            }

            for (int k = 0; k < propertyOfKey.length; k++) {
              valueOfKey[k] = currentRecord.getString(propertyOfKey[k]);
            }

            for (int k = 0; k < aggregateTablePropertyOfKey.length; k++) {
              aggregateTablePropertyOfKey[k] = rootEntity.getPrimaryKey().getInvolvedAttributes().get(k).getName();
            }
            // lookup
            fullRecord = this.getFullRecordByAggregateTable(aggregateTableRecords, aggregateTablePropertyOfKey, valueOfKey);

            // record imported if is not present in OrientDB
            Set<String> propertiesOfIndex = this.transformAggregateTablePropertyOfKey(aggregateTablePropertyOfKey, currentEntity);

            if (!graphDBCommandEngine
                .alreadyFullImportedInOrient(orientGraph, fullRecord, currentOutVertexType, propertiesOfIndex)) {

              currentOutVertex = (OVertex) graphDBCommandEngine
                  .upsertVisitedVertex(orientGraph, fullRecord, currentOutVertexType, propertiesOfIndex);

              // for each attribute of the entity belonging to the primary key, correspondent relationship is
              // built as edge and for the referenced record a vertex is built (only id)
              for (OCanonicalRelationship currentRelation : currentEntity.getAllOutCanonicalRelationships()) {

                currentParentEntity = mapper.getDataBaseSchema()
                    .getEntityByNameIgnoreCase(currentRelation.getParentEntity().getName());
                currentInVertexType = null; // reset for the current iteration

                // checking if parent table belongs to a hierarchical bag
                if (currentParentEntity.getHierarchicalBag() == null) {
                  currentInVertexType = mapper.getVertexTypeByEntity(currentRelation.getParentEntity());
                }

                // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
                else if (!currentEntity.getHierarchicalBag().equals(currentParentEntity.getHierarchicalBag())) {
                  propertyOfKey = new String[currentRelation.getFromColumns().size()];
                  valueOfKey = new String[currentRelation.getFromColumns().size()];

                  int index = 0;
                  for (OAttribute foreignAttribute : currentRelation.getFromColumns()) {
                    propertyOfKey[index] = currentRelation.getToColumns().get(index).getName();
                    valueOfKey[index] = fullRecord.getString((foreignAttribute.getName()));
                    index++;
                  }

                  // search is performed only if all the values in the foreign key are different from null (the relationship is inherited and is also consistent)
                  boolean ok = true;

                  for (int j = 0; j < valueOfKey.length; j++) {
                    if (valueOfKey[j] == null) {
                      ok = false;
                      break;
                    }
                  }
                  if (ok) {
                    String currentArrivalEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, null,
                        dbQueryEngine);
                    OEntity currentArrivalEntity = mapper.getDataBaseSchema().getEntityByName(currentArrivalEntityName);
                    currentInVertexType = mapper.getVertexTypeByEntity(currentArrivalEntity);
                  }
                }

                // if currentInVertexType is null then there isn't a relationship between to records, thus the edge will not be added.
                if (currentInVertexType != null) {
                  edgeType = mapper.getRelationship2edgeType().get(currentRelation);
                  graphDBCommandEngine
                      .upsertReachedVertexWithEdge(orientGraph, fullRecord, currentRelation, currentOutVertex, currentInVertexType,
                          edgeType.getName());
                }
              }
            }

            // closing aggregateTable result
            queryResult2.closeAll();

            // Statistics updated
            statistics.analyzedRecords++;

          }
          // closing resultset, connection and statement
          queryResult1.closeAll();
        }
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      OTeleporterContext.getInstance().getMessageHandler().info("\n");

      // setting the vertex type as 'analyzed'
      currentOutVertexType.setAnalyzedInLastMigration(true);

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }
  }

  /**
   * Performs import of all records of the entities contained in the hierarchical bag passed as parameter.
   * Adopted in case of "Table per Concrete Type" inheritance strategy.
   *
   * @param bag
   * @param dbQueryEngine
   * @param orientGraph
   */
  protected void tablePerConcreteTypeImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine,
      OGraphEngineForDB graphDBCommandEngine, ODatabaseDocument orientGraph) {

    try {

      OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;
      OVertexType currentInVertexType = null;
      OVertex currentOutVertex = null;
      OEdgeType edgeType = null;
      ResultSet records;
      ResultSet currentRecord;

      OQueryResult queryResult = null;

      for (int i = bag.getDepth2entities().size() - 1; i >= 0; i--) {
        for (OEntity currentEntity : bag.getDepth2entities().get(i)) {

          // for each entity in dbSchema all records are retrieved
          queryResult = dbQueryEngine.getRecordsByEntity(currentEntity);
          records = queryResult.getResult();
          currentRecord = null;

          currentOutVertexType = mapper.getVertexTypeByEntity(currentEntity);

          // each record is imported as vertex in the orient graph
          while (records.next()) {

            // upsert of the vertex
            currentRecord = records;

            // record imported if is not present in OrientDB
            String[] propertyOfKey = new String[currentEntity.getPrimaryKey().getInvolvedAttributes().size()];

            for (int k = 0; k < propertyOfKey.length; k++) {
              propertyOfKey[k] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(k).getName();
            }

            Set<String> propertiesOfIndex = this.transformAggregateTablePropertyOfKey(propertyOfKey,
                currentEntity);  // we need the key of original table, because we are working on it

            if (!graphDBCommandEngine
                .alreadyFullImportedInOrient(orientGraph, currentRecord, currentOutVertexType, propertiesOfIndex)) {

              currentOutVertex = (OVertex) graphDBCommandEngine
                  .upsertVisitedVertex(orientGraph, currentRecord, currentOutVertexType, propertiesOfIndex);

              // for each attribute of the entity belonging to the primary key, correspondent relationship is
              // built as edge and for the referenced record a vertex is built (only id)
              for (OCanonicalRelationship currentRelation : currentEntity.getAllOutCanonicalRelationships()) {

                currentParentEntity = mapper.getDataBaseSchema()
                    .getEntityByNameIgnoreCase(currentRelation.getParentEntity().getName());
                currentInVertexType = null; // reset for the current iteration

                // checking if parent table belongs to a hierarchical bag
                if (currentParentEntity.getHierarchicalBag() == null) {
                  currentInVertexType = mapper.getVertexTypeByEntity(currentRelation.getParentEntity());
                }

                // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
                else if (!currentEntity.getHierarchicalBag().equals(currentParentEntity.getHierarchicalBag())) {
                  propertyOfKey = new String[currentRelation.getFromColumns().size()];
                  String[] valueOfKey = new String[currentRelation.getFromColumns().size()];

                  int index = 0;
                  for (OAttribute foreignAttribute : currentRelation.getFromColumns()) {
                    propertyOfKey[index] = currentRelation.getToColumns().get(index).getName();
                    valueOfKey[index] = currentRecord.getString((foreignAttribute.getName()));
                    index++;
                  }

                  // search is performed only if all the values in the foreign key are different from null (the relationship is inherited and is also consistent)
                  boolean ok = true;

                  for (int j = 0; j < valueOfKey.length; j++) {
                    if (valueOfKey[j] == null) {
                      ok = false;
                      break;
                    }
                  }
                  if (ok) {
                    String currentArrivalEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, null,
                        dbQueryEngine);
                    OEntity currentArrivalEntity = mapper.getDataBaseSchema().getEntityByName(currentArrivalEntityName);
                    currentInVertexType = mapper.getVertexTypeByEntity(currentArrivalEntity);
                  }
                }

                // if currentInVertexType is null then there isn't a relationship between to records, thus the edge will not be added.
                if (currentInVertexType != null) {
                  edgeType = mapper.getRelationship2edgeType().get(currentRelation);
                  graphDBCommandEngine.upsertReachedVertexWithEdge(orientGraph, currentRecord, currentRelation, currentOutVertex,
                      currentInVertexType, edgeType.getName());
                }
              }
            }

            // Statistics updated
            statistics.analyzedRecords++;
          }
          // closing resultset, connection and statement
          queryResult.closeAll();
        }
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      OTeleporterContext.getInstance().getMessageHandler().info("\n");

      // setting the vertex type as 'analyzed'
      currentOutVertexType.setAnalyzedInLastMigration(true);

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }
  }

  private Set<String> transformAggregateTablePropertyOfKey(String[] aggregateTablePropertyOfKey, OEntity currentEntity) {
    Set<String> propertiesOfKey = new LinkedHashSet<String>();

    for (String tableKey : aggregateTablePropertyOfKey) {
      String correspondentProperty = ((OER2GraphMapper) this.mapper).getPropertyNameByEntityAndAttribute(currentEntity, tableKey);
      propertiesOfKey.add(correspondentProperty);
    }

    return propertiesOfKey;
  }

  /**
   * @param currentParentEntity
   * @param propertyOfKey
   * @param valueOfKey
   * @param physicalArrivalEntity
   * @param dbQueryEngine
   *
   * @return
   */
  private String searchParentEntityType(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey,
      OEntity physicalArrivalEntity, ODBQueryEngine dbQueryEngine) {

    switch (currentParentEntity.getHierarchicalBag().getInheritancePattern()) {

    case "table-per-hierarchy":
      return searchParentEntityTypeFromSingleTable(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntity,
          dbQueryEngine);

    case "table-per-type":
      return searchParentEntityTypeFromSubclassTable(currentParentEntity, propertyOfKey, valueOfKey, dbQueryEngine);

    case "table-per-concrete-type":
      return searchParentEntityTypeFromConcreteTable(currentParentEntity, propertyOfKey, valueOfKey, dbQueryEngine);

    }

    return null;
  }

  /**
   * @param currentParentEntity
   * @param valueOfKey
   * @param propertyOfKey
   * @param physicalEntity
   * @param dbQueryEngine
   *
   * @return
   */
  private String searchParentEntityTypeFromSingleTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey,
      OEntity physicalEntity, ODBQueryEngine dbQueryEngine) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String discriminatorColumn = hierarchicalBag.getDiscriminatorColumn();
    String entityName = null;

    try {

      OQueryResult queryResult = dbQueryEngine
          .getEntityTypeFromSingleTable(discriminatorColumn, physicalEntity, propertyOfKey, valueOfKey);
      ResultSet result = queryResult.getResult();
      result.next();
      String discriminatorValue = result.getString(discriminatorColumn);

      for (String currentEntityName : hierarchicalBag.getEntityName2discriminatorValue().keySet()) {
        if (hierarchicalBag.getEntityName2discriminatorValue().get(currentEntityName).equals(discriminatorValue)) {
          entityName = currentEntityName;
          break;
        }
      }

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    return entityName;
  }

  /**
   * @param currentParentEntity
   * @param propertyOfKey
   * @param valueOfKey
   * @param dbQueryEngine
   *
   * @return
   */
  private String searchParentEntityTypeFromSubclassTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey,
      ODBQueryEngine dbQueryEngine) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String entityName = null;

    try {

      OQueryResult queryResult = null;
      ResultSet result = null;

      for (int i = hierarchicalBag.getDepth2entities().size() - 1; i >= 0; i--) {
        for (OEntity currentEntity : hierarchicalBag.getDepth2entities().get(i)) {

          // Overwriting propertyOfKey with the currentEntity attributes' names
          for (int j = 0; j < currentEntity.getPrimaryKey().getInvolvedAttributes().size(); j++) {
            propertyOfKey[j] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(j).getName();
          }

          queryResult = dbQueryEngine.getRecordById(currentEntity, propertyOfKey, valueOfKey);
          result = queryResult.getResult();

          if (result != null) {
            entityName = currentEntity.getName();
            break;
          }
        }

        if (result != null) {
          break;
        }
      }

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    return entityName;
  }

  /**
   * @param currentParentEntity
   * @param propertyOfKey
   * @param valueOfKey
   * @param dbQueryEngine
   *
   * @return
   */
  private String searchParentEntityTypeFromConcreteTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey,
      ODBQueryEngine dbQueryEngine) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String entityName = null;

    try {

      OQueryResult queryResult = null;
      ResultSet result = null;

      for (int i = hierarchicalBag.getDepth2entities().size() - 1; i >= 0; i--) {
        for (OEntity currentEntity : hierarchicalBag.getDepth2entities().get(i)) {

          // Overwriting propertyOfKey with the currentEntity attributes' names
          for (int j = 0; j < currentEntity.getPrimaryKey().getInvolvedAttributes().size(); j++) {
            propertyOfKey[j] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(j).getName();
          }

          queryResult = dbQueryEngine.getRecordById(currentEntity, propertyOfKey, valueOfKey);
          result = queryResult.getResult();

          if (result != null) {
            entityName = currentEntity.getName();
            break;
          }
        }

        if (result != null) {
          break;
        }
      }

    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    return entityName;
  }

  /**
   * @param aggregateTableRecords
   * @param aggregateTablePropertyOfKey
   * @param valueOfKey
   *
   * @return
   */
  private ResultSet getFullRecordByAggregateTable(ResultSet aggregateTableRecords, String[] aggregateTablePropertyOfKey,
      String[] valueOfKey) {

    ResultSet fullRecord = null;
    String[] currentValueOfKey = new String[aggregateTablePropertyOfKey.length];
    boolean equals;

    try {

      while (aggregateTableRecords.next()) {

        for (int i = 0; i < aggregateTablePropertyOfKey.length; i++) {
          currentValueOfKey[i] = aggregateTableRecords.getString(aggregateTablePropertyOfKey[i]);
        }

        equals = true;
        for (int j = 0; j < valueOfKey.length; j++) {
          if (!valueOfKey[j].equals(currentValueOfKey[j])) {
            equals = false;
            break;
          }
        }

        if (equals) {
          fullRecord = aggregateTableRecords;
          return fullRecord;
        }

      }
    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    return fullRecord;
  }

  protected boolean hasGeospatialAttributes(OEntity entity, ODBMSDataTypeHandler handler) {

    for (OAttribute currentAttribute : entity.getAllAttributes()) {
      if (handler.isGeospatial(currentAttribute.getDataType()))
        return true;
    }

    return false;
  }

  protected String[][] buildAggregationColumnsFromAggregatedVertex(OConfiguredVertexClass configuredVertex) {

    String[][] columns;
    List<OSourceTable> sourceTables = configuredVertex.getMapping().getSourceTables();
    columns = new String[sourceTables.size()][sourceTables.get(0).getAggregationColumns().size()];
    int j = 0;
    for (OSourceTable currentSourceTable : sourceTables) {
      List<String> aggregationColumns = currentSourceTable.getAggregationColumns();

      if (aggregationColumns != null) {
        int k = 0;
        for (String attribute : aggregationColumns) {
          columns[j][k] = attribute;
          k++;
        }
      }
      j++;
    }
    return columns;
  }

}
