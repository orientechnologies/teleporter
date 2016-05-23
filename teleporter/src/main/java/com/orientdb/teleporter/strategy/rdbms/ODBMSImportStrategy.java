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

package com.orientdb.teleporter.strategy.rdbms;

import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.context.OTeleporterStatistics;
import com.orientdb.teleporter.exception.OTeleporterRuntimeException;
import com.orientdb.teleporter.factory.ODataTypeHandlerFactory;
import com.orientdb.teleporter.factory.ONameResolverFactory;
import com.orientdb.teleporter.factory.OQueryQuoteTypeFactory;
import com.orientdb.teleporter.importengine.rdbms.ODBQueryEngine;
import com.orientdb.teleporter.importengine.rdbms.OGraphEngineForDB;
import com.orientdb.teleporter.mapper.OSource2GraphMapper;
import com.orientdb.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientdb.teleporter.model.dbschema.OAttribute;
import com.orientdb.teleporter.model.dbschema.OEntity;
import com.orientdb.teleporter.model.dbschema.OHierarchicalBag;
import com.orientdb.teleporter.model.dbschema.ORelationship;
import com.orientdb.teleporter.model.graphmodel.OEdgeType;
import com.orientdb.teleporter.model.graphmodel.OVertexType;
import com.orientdb.teleporter.nameresolver.ONameResolver;
import com.orientdb.teleporter.persistence.handler.ODBMSDataTypeHandler;
import com.orientdb.teleporter.persistence.util.OQueryResult;
import com.orientdb.teleporter.strategy.OImportStrategy;
import com.orientdb.teleporter.util.OFileManager;
import com.orientdb.teleporter.util.OTimeFormatHandler;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public abstract class ODBMSImportStrategy implements OImportStrategy {

  // config info
  private final String configurationDirectoryName = "teleporter-config/";
  private final String configFileName = "config.json";
  private String outDBConfigPath;       // path ORIENTDB_HOME/<db-name>/teleporter-config/config.json
  private boolean configPresentInDB;

  public ODBMSImportStrategy() {}

  @Override
  public void executeStrategy(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, String nameResolverConvention,
      List<String> includedTables, List<String> excludedTables, String configurationPath, OTeleporterContext context) {

    Date globalStart = new Date();

    OQueryQuoteTypeFactory quoteFactory = new OQueryQuoteTypeFactory();
    quoteFactory.buildQueryQuoteType(driver, context);

    ODataTypeHandlerFactory factory = new ODataTypeHandlerFactory();
    ODBMSDataTypeHandler handler = (ODBMSDataTypeHandler) factory.buildDataTypeHandler(driver, context);

    /*
     * Step 1,2,3
     */
    ONameResolverFactory nameResolverFactory = new ONameResolverFactory();
    ONameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention, context);
    context.getStatistics().runningStepNumber = -1;

    // manage conf if present: loading
    ODocument config = this.loadConfiguration(outOrientGraphUri, configurationPath, context);

    OSource2GraphMapper mapper = this
        .createSchemaMapper(driver, uri, username, password, outOrientGraphUri, chosenMapper, xmlPath, nameResolver, handler,
            includedTables, excludedTables, config, context);

    // manage conf if present: updating in the db directory
    this.updateConfigurationInDatabase(config, configurationPath, context);

    // Step 4: Import
    this.executeImport(driver, uri, username, password, outOrientGraphUri, mapper, handler, context);
    context.getStatistics().notifyListeners();
    context.getOutputManager().info("");
    context.getStatistics().runningStepNumber = -1;

    Date globalEnd = new Date();

    context.getOutputManager().info("\n\nImporting complete in %s", OTimeFormatHandler.getHMSFormat(globalStart, globalEnd));
    context.getOutputManager().info(context.getStatistics().toString());

  }

  public abstract OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper,
      String xmlPath, ONameResolver nameResolver, ODBMSDataTypeHandler handler, List<String> includedTables, List<String> excludedTables,
      ODocument config, OTeleporterContext context);


  /**
   * Loading eventual configuration.
   * Look for the config in the <db-path>/teleporter-config/ path:
   *  (i) - if db and configuration are present use the default config
   *      - else
   *       (ii)  - if an external config path was passed as argument then load the config, use it for the steps 1,2,3 and then copy it
   *               in the <db-path>/teleporter-config/ path (configuration.json)
   *       (iii) - else execute strategy without configuration
   **/
  private ODocument loadConfiguration(String outOrientGraphUri, String configurationPath, OTeleporterContext context) {

    // checking the presence of the configuration in the target db
    if (!(outOrientGraphUri.charAt(outOrientGraphUri.length() - 1) == '/'
        || outOrientGraphUri.charAt(outOrientGraphUri.length() - 1) == '\\')) {
      outOrientGraphUri += "/";
    }
    this.outDBConfigPath = outOrientGraphUri + this.configurationDirectoryName + this.configFileName;
    this.outDBConfigPath = this.outDBConfigPath.replace("plocal:", "");
    File confFileInOrientDB = new File(this.outDBConfigPath);

    if (confFileInOrientDB.exists())
      this.configPresentInDB = true;
    else
      this.configPresentInDB = false;

    // (i)
    ODocument config;
    if (this.configPresentInDB) {
      config = OFileManager.buildJsonFromFile(this.outDBConfigPath);
      context.getOutputManager().error("Configuration correctly loaded from %s.", this.outDBConfigPath);
    } else {
      config = OFileManager.buildJsonFromFile(configurationPath);
      // (ii)
      if (config != null) {
        context.getOutputManager().error("Configuration correctly loaded from %s.", configurationPath);
      }
      // (iii)
      else {
        context.getOutputManager().error("No configuration file was found. Migration will be performed with standard mapping policies.");
      }
    }

    return config;
  }

  public void updateConfigurationInDatabase(ODocument config, String configurationPath, OTeleporterContext context) {
    // if we have config in input and it was not present in the DB then write it in the <db-path>/teleporter-config/ path
    if (config != null) {
      if (!this.configPresentInDB) {
        try {
          this.copyConfigIntoTargetDB(configurationPath, this.outDBConfigPath);
        } catch (IOException e) {
          if (e.getMessage() != null)
            context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
          else
            context.getOutputManager().error(e.getClass().getName());

          Writer writer = new StringWriter();
          e.printStackTrace(new PrintWriter(writer));
          String s = writer.toString();
          context.getOutputManager().error("\n" + s + "\n");
          throw new OTeleporterRuntimeException(e);
        }
      }
    }
  }

  public abstract void executeImport(String driver, String uri, String username, String password, String outOrientGraphUri, OSource2GraphMapper mapper, ODBMSDataTypeHandler handler, OTeleporterContext context);

  private void copyConfigIntoTargetDB(String sourceConfigPath, String destinationConfigPath) throws IOException {

    File sourceConfig = new File(sourceConfigPath);
    File destinationConfig = new File(destinationConfigPath);
    destinationConfig.getParentFile().mkdirs();
    destinationConfig.createNewFile();
    FileChannel in = new FileInputStream(sourceConfig).getChannel();
    FileChannel out = new FileOutputStream(destinationConfig).getChannel();
    out.transferFrom(in, 0, in.size());
  }

  /**
   * Performs import of all records of the entities contained in the hierarchical bag passed as parameter.
   * Adopted in case of "Table per Hierarchy" inheritance strategy.
   * @param bag
   * @param orientGraph
   * @param
   * @param context
   */
  protected void tablePerHierarchyImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine, OGraphEngineForDB graphDBCommandEngine, OrientBaseGraph orientGraph, OTeleporterContext context) {

    try {

      OTeleporterStatistics statistics = context.getStatistics();

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;
      OVertexType currentInVertexType = null;
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;

      String currentDiscriminatorValue;
      Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
      OEntity physicalCurrentEntity = it.next();
      String physicalCurrentEntityName = physicalCurrentEntity.getName();
      String physicalCurrentEntitySchemaName = physicalCurrentEntity.getSchemaName();

      OQueryResult queryResult = null;
      ResultSet records = null;

      for(int i=bag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: bag.getDepth2entities().get(i)) {
          currentDiscriminatorValue = bag.getEntityName2discriminatorValue().get(currentEntity.getName());

          // for each entity in dbSchema all records are retrieved
          queryResult = dbQueryEngine.getRecordsFromSingleTableByDiscriminatorValue(bag.getDiscriminatorColumn(), currentDiscriminatorValue, physicalCurrentEntityName, physicalCurrentEntitySchemaName, context);
          records = queryResult.getResult();
          ResultSet currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(currentEntity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {

            // upsert of the vertex
            currentRecord = records;
            currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(orientGraph, currentRecord, currentOutVertexType, null, context);

            // for each attribute of the entity belonging to the primary key, correspondent relationship is
            // built as edge and for the referenced record a vertex is built (only id)
            for(ORelationship currentRelation: currentEntity.getAllOutRelationships()) {

              currentParentEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase(currentRelation.getParentEntityName());

              // checking if parent table belongs to a hierarchical bag
              if(currentParentEntity.getHierarchicalBag() == null)
                currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));

                // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
              else {
                String[] propertyOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];
                String[] valueOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];

                int index = 0;
                for(OAttribute foreignAttribute: currentRelation.getForeignKey().getInvolvedAttributes())  {
                  propertyOfKey[index] = currentRelation.getPrimaryKey().getInvolvedAttributes().get(index).getName();
                  valueOfKey[index] = currentRecord.getString((foreignAttribute.getName()));
                  index++;
                }

                // search is performed only if all the values in the foreign key are different from null (the relationship is inherited and is also consistent)
                boolean ok = true;

                for(int j=0; j<valueOfKey.length; j++) {
                  if(valueOfKey[j] == null) {
                    ok = false;
                    break;
                  }
                }
                if(ok) {
                  it = currentParentEntity.getHierarchicalBag().getDepth2entities().get(0).iterator();
                  OEntity physicalArrivalEntity = it.next();
                  String physicalArrivalEntityName = physicalArrivalEntity.getName();
                  String physicalArrivalEntitySchemaName = physicalArrivalEntity.getSchemaName();
                  String currentArrivalEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntityName, physicalArrivalEntitySchemaName, dbQueryEngine, context);
                  currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentArrivalEntityName));
                }
              }

              // if currentInVertexType is null then there isn't a relationship between to records, thus the edge will not be added.
              if(currentInVertexType != null) {
                edgeType = mapper.getRelationship2edgeType().get(currentRelation);
                graphDBCommandEngine.upsertReachedVertexWithEdge(orientGraph, currentRecord, currentRelation, currentOutVertex, currentInVertexType, edgeType.getName(), context);
              }
            }

            // Statistics updated
            statistics.analyzedRecords++;
          }
          // closing resultset, connection and statement
          queryResult.closeAll(context);
        }
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      context.getOutputManager().info("");

    } catch(Exception e) {
      if(e.getMessage() != null)
        context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
      else
        context.getOutputManager().error(e.getClass().getName());

      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      String s = writer.toString();
      context.getOutputManager().error("\n" + s + "\n");
      throw new OTeleporterRuntimeException(e);
    }

  }


  /**
   * Performs import of all records of the entities contained in the hierarchical bag passed as parameter.
   * Adopted in case of "Table per Type" inheritance strategy.
   * @param bag
   * @param mapper
   * @param dbQueryEngine
   * @param graphDBCommandEngine
   * @param orientGraph
   * @param context
   */
  protected void tablePerTypeImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine, OGraphEngineForDB graphDBCommandEngine, OrientBaseGraph orientGraph, OTeleporterContext context) {

    try {

      OTeleporterStatistics statistics = context.getStatistics();

      ResultSet aggregateTableRecords = null;

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;
      OVertexType currentInVertexType = null;
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;
      ResultSet records;
      ResultSet currentRecord;
      ResultSet fullRecord;

      OQueryResult queryResult1 = null;
      OQueryResult queryResult2 = null;

      Iterator<OEntity> it = bag.getDepth2entities().get(0).iterator();
      OEntity rootEntity = it.next();

      for(int i=bag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: bag.getDepth2entities().get(i)) {

          // for each entity in dbSchema all records are retrieved
          queryResult1 = dbQueryEngine.getRecordsByEntity(currentEntity.getName(), currentEntity.getSchemaName(), context);
          records = queryResult1.getResult();
          currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(currentEntity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {
            queryResult2 = dbQueryEngine.buildAggregateTableFromHierarchicalBag(bag, context);
            aggregateTableRecords = queryResult2.getResult();

            // upsert of the vertex
            currentRecord = records;

            // lookup in the aggregateTable
            String[] propertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];
            String[] valueOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];
            String[] aggregateTablePropertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()];

            for(int k=0; k<propertyOfKey.length; k++) {
              propertyOfKey[k] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(k).getName();
            }

            for(int k=0; k<propertyOfKey.length; k++) {
              valueOfKey[k] = currentRecord.getString(propertyOfKey[k]);
            }

            for(int k=0; k<aggregateTablePropertyOfKey.length; k++) {
              aggregateTablePropertyOfKey[k] = rootEntity.getPrimaryKey().getInvolvedAttributes().get(k).getName();
            }
            // lookup
            fullRecord = this.getFullRecordByAggregateTable(aggregateTableRecords, aggregateTablePropertyOfKey, valueOfKey,context);

            // record imported if is not present in OrientDB
            Set<String> propertiesOfIndex = new LinkedHashSet<String>(Arrays.asList(aggregateTablePropertyOfKey));  // we need the key of the aggregate table, because we are working on it
            if(!graphDBCommandEngine.alreadyFullImportedInOrient(orientGraph, fullRecord, currentOutVertexType, propertiesOfIndex, context)) {

              currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(orientGraph, fullRecord, currentOutVertexType, propertiesOfIndex, context);

              // for each attribute of the entity belonging to the primary key, correspondent relationship is
              // built as edge and for the referenced record a vertex is built (only id)
              for(ORelationship currentRelation: currentEntity.getAllOutRelationships()) {

                currentParentEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase(currentRelation.getParentEntityName());
                currentInVertexType = null; // reset for the current iteration

                // checking if parent table belongs to a hierarchical bag
                if(currentParentEntity.getHierarchicalBag() == null)
                  currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));

                  // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
                else if(!currentEntity.getHierarchicalBag().equals(currentParentEntity.getHierarchicalBag())){
                  propertyOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];
                  valueOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];

                  int index = 0;
                  for(OAttribute foreignAttribute: currentRelation.getForeignKey().getInvolvedAttributes())  {
                    propertyOfKey[index] = currentRelation.getPrimaryKey().getInvolvedAttributes().get(index).getName();
                    valueOfKey[index] = fullRecord.getString((foreignAttribute.getName()));
                    index++;
                  }

                  // search is performed only if all the values in the foreign key are different from null (the relationship is inherited and is also consistent)
                  boolean ok = true;

                  for(int j=0; j<valueOfKey.length; j++) {
                    if(valueOfKey[j] == null) {
                      ok = false;
                      break;
                    }
                  }
                  if(ok) {
                    String currentArrivalEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, null, null, dbQueryEngine, context);
                    currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentArrivalEntityName));
                  }
                }

                // if currentInVertexType is null then there isn't a relationship between to records, thus the edge will not be added.
                if(currentInVertexType != null) {
                  edgeType = mapper.getRelationship2edgeType().get(currentRelation);
                  graphDBCommandEngine.upsertReachedVertexWithEdge(orientGraph, fullRecord, currentRelation, currentOutVertex, currentInVertexType, edgeType.getName(), context);
                }
              }
            }

            // closing aggregateTable result
            queryResult2.closeAll(context);

            // Statistics updated
            statistics.analyzedRecords++;

          }
          // closing resultset, connection and statement
          queryResult1.closeAll(context);
        }
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      context.getOutputManager().info("");

    } catch(Exception e) {
      if(e.getMessage() != null)
        context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
      else
        context.getOutputManager().error(e.getClass().getName());

      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      String s = writer.toString();
      context.getOutputManager().error("\n" + s + "\n");
      throw new OTeleporterRuntimeException(e);
    }
  }



  /**
   * Performs import of all records of the entities contained in the hierarchical bag passed as parameter.
   * Adopted in case of "Table per Concrete Type" inheritance strategy.
   * @param bag
   * @param dbQueryEngine
   * @param orientGraph
   * @param context
   */
  protected void tablePerConcreteTypeImport(OHierarchicalBag bag, OER2GraphMapper mapper, ODBQueryEngine dbQueryEngine, OGraphEngineForDB graphDBCommandEngine, OrientBaseGraph orientGraph, OTeleporterContext context) {

    try {

      OTeleporterStatistics statistics = context.getStatistics();

      OEntity currentParentEntity = null;

      OVertexType currentOutVertexType = null;
      OVertexType currentInVertexType = null;
      OrientVertex currentOutVertex = null;
      OEdgeType edgeType = null;
      ResultSet records;
      ResultSet currentRecord;

      OQueryResult queryResult = null;

      for(int i=bag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: bag.getDepth2entities().get(i)) {

          // for each entity in dbSchema all records are retrieved
          queryResult = dbQueryEngine.getRecordsByEntity(currentEntity.getName(), currentEntity.getSchemaName(), context);
          records = queryResult.getResult();
          currentRecord = null;

          currentOutVertexType = mapper.getEntity2vertexType().get(currentEntity);

          // each record is imported as vertex in the orient graph
          while(records.next()) {

            // upsert of the vertex
            currentRecord = records;

            // record imported if is not present in OrientDB
            String[] propertyOfKey = new String[currentEntity.getPrimaryKey().getInvolvedAttributes().size()];

            for(int k=0; k<propertyOfKey.length; k++) {
              propertyOfKey[k] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(k).getName();
            }

            Set<String> propertiesOfIndex = new LinkedHashSet<String>(Arrays.asList(propertyOfKey));  // we need the key of original table, because we are working on it

            if(!graphDBCommandEngine.alreadyFullImportedInOrient(orientGraph, currentRecord, currentOutVertexType, propertiesOfIndex, context)) {

              currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(orientGraph, currentRecord, currentOutVertexType, propertiesOfIndex, context);

              // for each attribute of the entity belonging to the primary key, correspondent relationship is
              // built as edge and for the referenced record a vertex is built (only id)
              for(ORelationship currentRelation: currentEntity.getAllOutRelationships()) {

                currentParentEntity = mapper.getDataBaseSchema().getEntityByNameIgnoreCase(currentRelation.getParentEntityName());
                currentInVertexType = null; // reset for the current iteration

                // checking if parent table belongs to a hierarchical bag
                if(currentParentEntity.getHierarchicalBag() == null)
                  currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));

                  // if the parent entity belongs to hierarchical bag, we need to know which is it the more stringent subclass of the record with a certain id
                else if(!currentEntity.getHierarchicalBag().equals(currentParentEntity.getHierarchicalBag())) {
                  propertyOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];
                  String[] valueOfKey = new String[currentRelation.getForeignKey().getInvolvedAttributes().size()];

                  int index = 0;
                  for(OAttribute foreignAttribute: currentRelation.getForeignKey().getInvolvedAttributes())  {
                    propertyOfKey[index] = currentRelation.getPrimaryKey().getInvolvedAttributes().get(index).getName();
                    valueOfKey[index] = currentRecord.getString((foreignAttribute.getName()));
                    index++;
                  }

                  // search is performed only if all the values in the foreign key are different from null (the relationship is inherited and is also consistent)
                  boolean ok = true;

                  for(int j=0; j<valueOfKey.length; j++) {
                    if(valueOfKey[j] == null) {
                      ok = false;
                      break;
                    }
                  }
                  if(ok) {
                    String currentArrivalEntityName = searchParentEntityType(currentParentEntity, propertyOfKey, valueOfKey, null, null, dbQueryEngine, context);
                    currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentArrivalEntityName));
                  }
                }

                // if currentInVertexType is null then there isn't a relationship between to records, thus the edge will not be added.
                if(currentInVertexType != null) {
                  edgeType = mapper.getRelationship2edgeType().get(currentRelation);
                  graphDBCommandEngine.upsertReachedVertexWithEdge(orientGraph, currentRecord, currentRelation, currentOutVertex, currentInVertexType, edgeType.getName(), context);
                }
              }
            }

            // Statistics updated
            statistics.analyzedRecords++;
          }
          // closing resultset, connection and statement
          queryResult.closeAll(context);
        }
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      context.getOutputManager().info("");

    } catch(Exception e) {
      if(e.getMessage() != null)
        context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
      else
        context.getOutputManager().error(e.getClass().getName());

      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      String s = writer.toString();
      context.getOutputManager().error("\n" + s + "\n");
      throw new OTeleporterRuntimeException(e);
    }
  }


  /**
   * @param currentParentEntity
   * @param propertyOfKey
   * @param valueOfKey
   * @param physicalArrivalEntityName
   * @param physicalArrivalEntitySchemaName
   * @param dbQueryEngine
   * @param context
   * @return
   */
  private String searchParentEntityType(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, String physicalArrivalEntityName, String physicalArrivalEntitySchemaName, ODBQueryEngine dbQueryEngine, OTeleporterContext context) {

    switch(currentParentEntity.getHierarchicalBag().getInheritancePattern()) {

    case "table-per-hierarchy": return searchParentEntityTypeFromSingleTable(currentParentEntity, propertyOfKey, valueOfKey, physicalArrivalEntityName, physicalArrivalEntitySchemaName, dbQueryEngine, context);

    case "table-per-type": return searchParentEntityTypeFromSubclassTable(currentParentEntity, propertyOfKey, valueOfKey, dbQueryEngine, context);

    case "table-per-concrete-type": return searchParentEntityTypeFromConcreteTable(currentParentEntity, propertyOfKey, valueOfKey, dbQueryEngine, context);

    }

    return null;
  }



  /**
   * @param currentParentEntity
   * @param valueOfKey
   * @param propertyOfKey
   * @param physicalEntityName
   * @param dbQueryEngine
   * @param context
   * @return
   */
  private String searchParentEntityTypeFromSingleTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, String physicalEntityName, String physicalEntitySchemaName, ODBQueryEngine dbQueryEngine, OTeleporterContext context) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String discriminatorColumn = hierarchicalBag.getDiscriminatorColumn();
    String entityName = null;

    try {

      OQueryResult queryResult = dbQueryEngine.getEntityTypeFromSingleTable(discriminatorColumn, physicalEntityName, physicalEntitySchemaName, propertyOfKey, valueOfKey, context);
      ResultSet result = queryResult.getResult();
      result.next();
      String discriminatorValue = result.getString(discriminatorColumn);

      for(String currentEntityName: hierarchicalBag.getEntityName2discriminatorValue().keySet()) {
        if(hierarchicalBag.getEntityName2discriminatorValue().get(currentEntityName).equals(discriminatorValue)) {
          entityName = currentEntityName;
          break;
        }
      }

    } catch(Exception e) {
      if(e.getMessage() != null)
        context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
      else
        context.getOutputManager().error(e.getClass().getName());

      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      String s = writer.toString();
      context.getOutputManager().error("\n" + s + "\n");
      throw new OTeleporterRuntimeException(e);
    }

    return entityName;
  }


  /**
   * @param currentParentEntity
   * @param propertyOfKey
   * @param valueOfKey
   * @param dbQueryEngine
   * @param context
   * @return
   */
  private String searchParentEntityTypeFromSubclassTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, ODBQueryEngine dbQueryEngine, OTeleporterContext context) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String entityName = null;

    try {

      OQueryResult queryResult = null;
      ResultSet result = null;

      for(int i=hierarchicalBag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: hierarchicalBag.getDepth2entities().get(i)) {

          // Overwriting propertyOfKey with the currentEntity attributes' names
          for(int j=0; j<currentEntity.getPrimaryKey().getInvolvedAttributes().size(); j++) {
            propertyOfKey[j] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(j).getName();
          }

          queryResult = dbQueryEngine.getRecordById(currentEntity.getName(), currentEntity.getSchemaName(), propertyOfKey, valueOfKey, context);
          result = queryResult.getResult();

          if(result != null) {
            entityName = currentEntity.getName();
            break;
          }
        }

        if(result != null) {
          break;
        }
      }

    } catch(Exception e) {
      if(e.getMessage() != null)
        context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
      else
        context.getOutputManager().error(e.getClass().getName());

      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      String s = writer.toString();
      context.getOutputManager().error("\n" + s + "\n");
      throw new OTeleporterRuntimeException(e);
    }

    return entityName;
  }


  /**
   * @param currentParentEntity
   * @param propertyOfKey
   * @param valueOfKey
   * @param dbQueryEngine
   * @param context
   * @return
   */
  private String searchParentEntityTypeFromConcreteTable(OEntity currentParentEntity, String[] propertyOfKey, String[] valueOfKey, ODBQueryEngine dbQueryEngine, OTeleporterContext context) {

    OHierarchicalBag hierarchicalBag = currentParentEntity.getHierarchicalBag();
    String entityName = null;

    try {

      OQueryResult queryResult = null;
      ResultSet result = null;

      for(int i=hierarchicalBag.getDepth2entities().size()-1; i>=0; i--) {
        for(OEntity currentEntity: hierarchicalBag.getDepth2entities().get(i)) {

          // Overwriting propertyOfKey with the currentEntity attributes' names
          for(int j=0; j<currentEntity.getPrimaryKey().getInvolvedAttributes().size(); j++) {
            propertyOfKey[j] = currentEntity.getPrimaryKey().getInvolvedAttributes().get(j).getName();
          }

          queryResult = dbQueryEngine.getRecordById(currentEntity.getName(), currentEntity.getSchemaName(), propertyOfKey, valueOfKey, context);
          result = queryResult.getResult();

          if(result != null) {
            entityName = currentEntity.getName();
            break;
          }
        }

        if(result != null) {
          break;
        }
      }

    } catch(Exception e) {
      if(e.getMessage() != null)
        context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
      else
        context.getOutputManager().error(e.getClass().getName());

      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      String s = writer.toString();
      context.getOutputManager().error("\n" + s + "\n");
      throw new OTeleporterRuntimeException(e);
    }

    return entityName;
  }


  /**
   * @param aggregateTableRecords
   * @param aggregateTablePropertyOfKey
   * @param valueOfKey
   * @param context
   * @return
   */
  private ResultSet getFullRecordByAggregateTable(ResultSet aggregateTableRecords, String[] aggregateTablePropertyOfKey, String[] valueOfKey, OTeleporterContext context) {

    ResultSet fullRecord = null;
    String[] currentValueOfKey = new String[aggregateTablePropertyOfKey.length];
    boolean equals;

    try {

      while(aggregateTableRecords.next()) {

        for(int i=0; i<aggregateTablePropertyOfKey.length; i++) {
          currentValueOfKey[i] = aggregateTableRecords.getString(aggregateTablePropertyOfKey[i]);
        }

        equals = true;
        for(int j=0; j<valueOfKey.length; j++) {
          if(!valueOfKey[j].equals(currentValueOfKey[j])) {
            equals = false;
            break;
          }
        }

        if(equals) {
          fullRecord = aggregateTableRecords;
          return fullRecord;
        }

      }
    } catch(Exception e) {
      if(e.getMessage() != null)
        context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
      else
        context.getOutputManager().error(e.getClass().getName());

      Writer writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      String s = writer.toString();
      context.getOutputManager().error("\n" + s + "\n");
      throw new OTeleporterRuntimeException(e);
    }

    return fullRecord;
  }


  protected boolean hasGeospatialAttributes(OEntity entity, ODBMSDataTypeHandler handler) {

    for(OAttribute currentAttribute: entity.getAllAttributes()) {
      if(handler.isGeospatial(currentAttribute.getDataType()))
        return true;
    }

    return false;
  }


}
