/*
 * Copyright 2016 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.teleporter.configuration;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.api.*;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.mapper.rdbms.OAggregatorEdge;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.rdbms.classmapper.OClassMapper;
import com.orientechnologies.teleporter.model.dbschema.OAttribute;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.ORelationship;
import com.orientechnologies.teleporter.model.dbschema.OSourceDatabaseInfo;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OModelProperty;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;

import java.util.*;

/**
 * It handles the configuration of the Teleporter workflow. It allows:
 * 1. translation from OConfiguration object to JSON format and vice versa.
 * 2. the OConfiguration object building starting from a mapping between dbms and graph models.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 *
 */

public class OConfigurationHandler {

    private boolean runningAggregationStrategy;

    public OConfigurationHandler(boolean runningAggregationStrategy) {
        this.runningAggregationStrategy = runningAggregationStrategy;
    }

    /**
     * Parsing method. It returns a OConfiguration object starting from a JSON configuration.
     *
     * @param jsonConfiguration
     * @return
     */

    public OConfiguration buildConfigurationFromJSONDoc(ODocument jsonConfiguration) {

        OConfiguration configuration = new OConfiguration();

        // parsing vertices' migrationConfigDoc
        this.buildConfiguredVertices(jsonConfiguration, configuration);

        // parsing edges' migrationConfigDoc
        this.buildConfiguredEdges(jsonConfiguration, configuration);

        return configuration;
    }


    private void buildConfiguredVertices(ODocument jsonConfiguration, OConfiguration configuration) {

        List<ODocument> verticesDoc = jsonConfiguration.field("vertices");
        List<OConfiguredVertexClass> configuredVertices = new LinkedList<OConfiguredVertexClass>();

        if (verticesDoc == null) {
            OTeleporterContext.getInstance().getOutputManager().error("Configuration error: 'vertices' field not found.");
            throw new OTeleporterRuntimeException();
        }

        // building vertices
        for (ODocument currentVertexDoc : verticesDoc) {

            String configuredVertexClassName = currentVertexDoc.field("name");
            if(configuredVertexClassName == null) {
                OTeleporterContext.getInstance().getOutputManager().error("Configuration error: 'name' field not found in vertex class definition.");
                throw new OTeleporterRuntimeException();
            }

            OConfiguredVertexClass currentConfiguredVertex = new OConfiguredVertexClass(configuredVertexClassName);
            OVertexMappingInformation currentMapping = new OVertexMappingInformation(currentConfiguredVertex);
            ODocument mappingDoc = currentVertexDoc.field("mapping");

            if(mappingDoc == null) {
                OTeleporterContext.getInstance().getOutputManager().error("Configuration error: 'mapping' field not found in the '%s' vertex-type definition.",  configuredVertexClassName);
                throw new OTeleporterRuntimeException();
            }

            /**
             * Mapping building
             */

            // Aggregation function (optional)
            String aggregationFunction = mappingDoc.field("aggregationFunction");
            currentMapping.setAggregationFunction(aggregationFunction);

            // Source Tables building (mandatory)
            List<ODocument> sourceTableDocs = mappingDoc.field("sourceTables");
            if(sourceTableDocs == null) {
                OTeleporterContext.getInstance().getOutputManager().error("Configuration error: 'sourceTables' field not found in the '%s' vertex-type mapping.",  configuredVertexClassName);
                throw new OTeleporterRuntimeException();
            }

            List<OSourceTable> sourceTables = new LinkedList<OSourceTable>();

            Map<String, String> sourceId2tableName = new HashMap<String, String>();

            int i = 0;
            for (ODocument sourceTable : sourceTableDocs) {
                String sourceIdName = sourceTable.field("name");
                String sourceTableName = sourceTable.field("tableName");
                String dataSource = sourceTable.field("dataSource");
                if(sourceIdName == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'name' field not found in the '%s' vertex-class mapping with the source table.",  configuredVertexClassName);
                    throw new OTeleporterRuntimeException();
                }
                if(sourceTableName == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'tableName' field not found in the '%s' vertex-class mapping with the source table.",  configuredVertexClassName);
                    throw new OTeleporterRuntimeException();
                }
                if(dataSource == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'dataSource' field not found in the '%s' vertex-class mapping with the source table.",  configuredVertexClassName);
                    throw new OTeleporterRuntimeException();
                }
                sourceId2tableName.put(sourceIdName, sourceTableName);

                List<String> aggregationColumns = sourceTable.field("aggregationColumns");

                if(aggregationFunction != null && aggregationColumns == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'aggregationColumns' field not found in the '%s' vertex-class mapping with the source table " +
                            "('aggregationFunction' field is stated, thus 'aggregationColumns' field is expected for each source table).",  configuredVertexClassName);
                    throw new OTeleporterRuntimeException();
                }

                OSourceTable currentSourceTable = new OSourceTable(sourceIdName);
                currentSourceTable.setDataSource(dataSource);
                currentSourceTable.setTableName(sourceTableName);
                if(aggregationFunction != null && aggregationColumns != null) {
                    currentSourceTable.setAggregationColumns(aggregationColumns);
                }
                sourceTables.add(currentSourceTable);

                i++;
            }
            currentMapping.setSourceTables(sourceTables);
            currentConfiguredVertex.setMapping(currentMapping);

            /**
             *  extract and set configured properties
             */
            List<OConfiguredProperty> configuredProperties = this.extractProperties(currentVertexDoc, configuredVertexClassName);
            currentConfiguredVertex.setConfiguredProperties(configuredProperties);
            configuredVertices.add(currentConfiguredVertex);
        }

        configuration.setConfiguredVertices(configuredVertices);
    }

    private void buildConfiguredEdges(ODocument jsonConfiguration, OConfiguration configuration) {

        List<ODocument> edgesDoc = jsonConfiguration.field("edges");
        List<OConfiguredEdgeClass> configuredEdges = new LinkedList<OConfiguredEdgeClass>();

        if(edgesDoc == null) {
            OTeleporterContext.getInstance().getOutputManager().error("Configuration error: 'edges' field not found.");
            throw new OTeleporterRuntimeException();
        }

        // building edges
        for(ODocument currentEdgeDoc: edgesDoc) {

            String[] edgeFields = currentEdgeDoc.fieldNames();
            if(edgeFields.length != 1) {
                OTeleporterContext.getInstance().getOutputManager().error("Configuration error: wrong edge definition.");
            }
            String configuredEdgeClassName = edgeFields[0];
            OConfiguredEdgeClass currentConfiguredEdge = new OConfiguredEdgeClass(configuredEdgeClassName);
            OAggregatedJoinTableMapping joinTableMapping = null;
            List<OEdgeMappingInformation> edgeMappings = new LinkedList<OEdgeMappingInformation>();
            ODocument currentEdgeInfo = currentEdgeDoc.field(configuredEdgeClassName);
            List<ODocument> mappingDocs = currentEdgeInfo.field("mapping");

            // building configured edges
            if(mappingDocs == null) {
                OTeleporterContext.getInstance().getOutputManager().error("Configuration error: 'mapping' field not found in the '%s' edge-type definition.",  configuredEdgeClassName);
                throw new OTeleporterRuntimeException();
            }

            // for each relationship represented by the edge a mapping is built
            for(ODocument mappingDoc: mappingDocs) {

                OEdgeMappingInformation currentMapping = new OEdgeMappingInformation(currentConfiguredEdge);
                String currentForeignEntityName = mappingDoc.field("fromTable");
                String currentParentEntityName = mappingDoc.field("toTable");
                List<String> fromColumns = mappingDoc.field("fromColumns");
                List<String> toColumns = mappingDoc.field("toColumns");

                ODocument joinTableDoc = mappingDoc.field("joinTable");

                // migrationConfigDoc errors managing (draconian approach)
                if (currentForeignEntityName == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'fromTable' field not found in the '%s' edge-type mapping.", configuredEdgeClassName);
                    throw new OTeleporterRuntimeException();
                }
                if (currentParentEntityName == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'toTable' field not found in the '%s' edge-type mapping.", configuredEdgeClassName);
                    throw new OTeleporterRuntimeException();
                }
                if (fromColumns == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'fromColumns' field not found in the '%s' edge-type mapping.", configuredEdgeClassName);
                    throw new OTeleporterRuntimeException();
                }
                if (toColumns == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'toColumns' field not found in the '%s' edge-type mapping.", configuredEdgeClassName);
                    throw new OTeleporterRuntimeException();
                }

                String direction = mappingDoc.field("direction");

                if (direction != null && !(direction.equals("direct") || direction.equals("inverse"))) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: direction for the edge %s cannot be '%s'. Allowed values: 'direct' or 'inverse' \n", configuredEdgeClassName, direction);
                    throw new OTeleporterRuntimeException();
                }

                boolean foreignEntityIsJoinTableToAggregate = false;

                if (joinTableDoc != null) {

                    String joinTableName = joinTableDoc.field("tableName");

                    if (joinTableName == null) {
                        OTeleporterContext.getInstance().getOutputManager()
                                .error("Configuration error: 'tableName' field not found in the join table mapping with the '%s' edge-type.", configuredEdgeClassName);
                        throw new OTeleporterRuntimeException();
                    }

                    joinTableMapping = new OAggregatedJoinTableMapping(joinTableName);

                    if (this.runningAggregationStrategy) {
                        // strategy is aggregated
                        List<String> joinTableFromColumns = joinTableDoc.field("fromColumns");
                        List<String> joinTableToColumns = joinTableDoc.field("toColumns");

                        if (joinTableFromColumns == null) {
                            OTeleporterContext.getInstance().getOutputManager()
                                    .error("Configuration error: 'fromColumns' field not found in the join table mapping with the '%s' edge-type.", configuredEdgeClassName);
                            throw new OTeleporterRuntimeException();
                        }
                        if (joinTableToColumns == null) {
                            OTeleporterContext.getInstance().getOutputManager()
                                    .error("Configuration error: 'toColumns' field not found in the join table mapping with the '%s' edge-type.", configuredEdgeClassName);
                            throw new OTeleporterRuntimeException();
                        }

                        joinTableMapping.setFromColumns(joinTableFromColumns);
                        joinTableMapping.setToColumns(joinTableToColumns);
                    } else if (OTeleporterContext.getInstance().getExecutionStrategy().equals("naive")) {
                        OTeleporterContext.getInstance().getOutputManager()
                                .error("Configuration not compliant with the chosen strategy: you cannot perform the aggregation declared in the migrationConfigDoc for the "
                                + "join table %s while executing migration with a not-aggregating strategy. Thus no aggregation will be performed.\n", joinTableName);
                        throw new OTeleporterRuntimeException();
                    }
                }

                // Updating edge's migrationConfigDoc
                currentMapping.setFromTableName(currentForeignEntityName);
                currentMapping.setToTableName(currentParentEntityName);
                currentMapping.setFromColumns(fromColumns);
                currentMapping.setToColumns(toColumns);
                currentMapping.setDirection(direction);
                currentMapping.setRepresentedJoinTableMapping(joinTableMapping);
                edgeMappings.add(currentMapping);
            }

            ((OConfiguredEdgeClass)currentConfiguredEdge).setMappings(edgeMappings);

            // extract and set configured properties
            List<OConfiguredProperty> configuredProperties = this.extractProperties(currentEdgeInfo, configuredEdgeClassName);
            currentConfiguredEdge.setConfiguredProperties(configuredProperties);
            configuredEdges.add(currentConfiguredEdge);
        }
        configuration.setConfiguredEdges(configuredEdges);
    }

    private List<OConfiguredProperty> extractProperties(ODocument currentElementInfo, String className) {

        // extracting properties info if present and adding them to the current edge-type
        ODocument elementPropsDoc = currentElementInfo.field("properties");

        List<OConfiguredProperty> configuredProperties = new LinkedList<OConfiguredProperty>();

        // adding properties to the edge
        if(elementPropsDoc != null) {
            String[] propertiesFields = elementPropsDoc.fieldNames();

            for(String propertyName: propertiesFields) {
                ODocument currentElementPropertyDoc = elementPropsDoc.field(propertyName);
                Boolean isIncludedInMigration = currentElementPropertyDoc.field("include");
                String propertyType = currentElementPropertyDoc.field("type");
                if(isIncludedInMigration == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'include' field not found in the '%s' property definition ('%s' Class).",  propertyName, className);
                    throw new OTeleporterRuntimeException();
                }
                if(propertyType == null) {
                    OTeleporterContext.getInstance().getStatistics()
                            .warningMessages.add("Configuration ERROR: the property " + propertyName + " will not added to the correspondent Class because the type is badly defined or not defined at all.");
                    continue;
                }
                else {
                    propertyType = propertyType.toUpperCase(Locale.ENGLISH);        // normalization: orientdb types are upper case
                }

                Boolean mandatory = currentElementPropertyDoc.field("mandatory");
                Boolean readOnly = currentElementPropertyDoc.field("readOnly");
                Boolean notNull = currentElementPropertyDoc.field("notNull");

                if(mandatory == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'mandatory' field not found in the '%s' property definition ('%s' Class).",  propertyName, className);
                    throw new OTeleporterRuntimeException();
                }
                if(readOnly == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'readOnly' field not found in the '%s' property definition ('%s' Class).",  propertyName, className);
                    throw new OTeleporterRuntimeException();
                }
                if(notNull == null) {
                    OTeleporterContext.getInstance().getOutputManager()
                            .error("Configuration error: 'notNull' field not found in the '%s' property definition ('%s' Class).",  propertyName, className);
                    throw new OTeleporterRuntimeException();
                }

                // extracting mapping info (optional: may be null if the property is defined from scratch (only schema definition))
                ODocument propertyMappingInfo = currentElementPropertyDoc.field("mapping");
                OConfiguredPropertyMapping propertyMapping = null;
                if(propertyMappingInfo != null) {
                    String source = propertyMappingInfo.field("source");
                    String columnName = propertyMappingInfo.field("columnName");
                    String type = propertyMappingInfo.field("type");
                    if(source == null) {
                        OTeleporterContext.getInstance().getOutputManager()
                                .error("Configuration error: 'source' field not found in the '%s' property mapping ('%s' Class).",  propertyName, className);
                        throw new OTeleporterRuntimeException();
                    }
                    if(columnName == null) {
                        OTeleporterContext.getInstance().getOutputManager()
                                .error("Configuration error: 'columnName' field not found in the '%s' property mapping ('%s' Class).",  propertyName, className);
                        throw new OTeleporterRuntimeException();
                    }
                    if(type == null) {
                        OTeleporterContext.getInstance().getOutputManager()
                                .error("Configuration error: 'type' field not found in the '%s' property mapping ('%s' Class).",  propertyName, className);
                        throw new OTeleporterRuntimeException();
                    }
                    propertyMapping = new OConfiguredPropertyMapping(source);
                    propertyMapping.setColumnName(columnName);
                    propertyMapping.setType(type);
                }

                OConfiguredProperty currentConfiguredProperty = new OConfiguredProperty(propertyName);
                currentConfiguredProperty.setIncludedInMigration(isIncludedInMigration);
                currentConfiguredProperty.setPropertyType(propertyType);
                currentConfiguredProperty.setMandatory(mandatory);
                currentConfiguredProperty.setReadOnly(readOnly);
                currentConfiguredProperty.setNotNull(notNull);
                currentConfiguredProperty.setPropertyMapping(propertyMapping);

                configuredProperties.add(currentConfiguredProperty);
            }
        }
        return configuredProperties;
    }

    /**
     * It returns a ODocument object containing the configuration (JSON format) starting from a OConfiguration object.
     *
     * @param configuration
     * @return
     */
    public ODocument buildJSONDocFromConfiguration(OConfiguration configuration) {

        ODocument jsonConfiguration = new ODocument();

        // writing configured vertices
        this.writeConfiguredVerticesOnJsonDocument(configuration, jsonConfiguration);

        // writing configured edges
        this.writeConfiguredEdgesOnJsonDocument(configuration, jsonConfiguration);

        return jsonConfiguration;
    }


    private void writeConfiguredVerticesOnJsonDocument(OConfiguration configuration, ODocument jsonConfiguration) {
        List<ODocument> vertices = new LinkedList<ODocument>();

        for(OConfiguredVertexClass currConfiguredVertex: configuration.getConfiguredVertices()) {
            ODocument currVertexDoc = new ODocument();
            currVertexDoc.field("name", currConfiguredVertex.getName());

            /*
             * Setting vertex mapping
             */

            OVertexMappingInformation currVertexMapping = currConfiguredVertex.getMapping();
            ODocument currVertexMappingDoc = new ODocument();

            // source tables
            List<OSourceTable> sourceTables = currVertexMapping.getSourceTables();
            List<ODocument> sourceTablesDoc = new LinkedList<ODocument>();
            for(OSourceTable currSourceTable: sourceTables) {
                ODocument currSourceTableDoc = new ODocument();
                currSourceTableDoc.field("name", currSourceTable.getSourceIdName());
                currSourceTableDoc.field("dataSource", currSourceTable.getDataSource());
                currSourceTableDoc.field("tableName", currSourceTable.getTableName());
                List<String> aggregationColumns = currSourceTable.getAggregationColumns();
                if(aggregationColumns != null) {
                    currSourceTableDoc.field("aggregationColumns", aggregationColumns);
                }
                sourceTablesDoc.add(currSourceTableDoc);
            }
            currVertexMappingDoc.field("sourceTables", sourceTablesDoc);

            // aggregation function
            String aggregationFunction = currVertexMapping.getAggregationFunction();
            if(aggregationFunction != null) {
                currVertexMappingDoc.field("aggregationFunction", aggregationFunction);
            }
            currVertexDoc.field("mapping", currVertexMappingDoc);


            /*
             * Setting properties
             */

            ODocument propertiesDoc = this.writeConfiguredProperties(currConfiguredVertex.getConfiguredProperties());
            currVertexDoc.field("properties", propertiesDoc);

            vertices.add(currVertexDoc);
        }

        // collecting configured vertices
        jsonConfiguration.field("vertices", vertices);
    }


    private void writeConfiguredEdgesOnJsonDocument(OConfiguration configuration, ODocument jsonConfiguration) {
        List<ODocument> edges = new LinkedList<ODocument>();

        for(OConfiguredEdgeClass currConfiguredEdge: configuration.getConfiguredEdges()) {
            ODocument currEdgeDoc = new ODocument();
            ODocument currEdgeInfoDoc = new ODocument();
            String edgeClassName = currConfiguredEdge.getName();

            /*
             * Setting edge mapping
             */

            List<OEdgeMappingInformation> edgeMappings = currConfiguredEdge.getMappings();
            List<ODocument> edgeMappingDocs = new LinkedList<ODocument>();
            for(OEdgeMappingInformation currEdgeMapping: edgeMappings) {
                ODocument currEdgeMappingDoc = new ODocument();
                currEdgeMappingDoc.field("fromTable", currEdgeMapping.getFromTableName());
                currEdgeMappingDoc.field("fromColumns", currEdgeMapping.getFromColumns());
                currEdgeMappingDoc.field("toTable", currEdgeMapping.getToTableName());
                currEdgeMappingDoc.field("toColumns", currEdgeMapping.getToColumns());
                OAggregatedJoinTableMapping representedJoinTableMapping = currEdgeMapping.getRepresentedJoinTableMapping();
                if (representedJoinTableMapping != null) {
                    ODocument joinTableMapping = new ODocument();
                    joinTableMapping.field("tableName", representedJoinTableMapping.getTableName());
                    joinTableMapping.field("fromColumns", representedJoinTableMapping.getFromColumns());
                    joinTableMapping.field("toColumns", representedJoinTableMapping.getToColumns());
                    currEdgeMappingDoc.field("joinTable", joinTableMapping);
                }
                currEdgeMappingDoc.field("direction", currEdgeMapping.getDirection());
                edgeMappingDocs.add(currEdgeMappingDoc);
            }

            currEdgeInfoDoc.field("mapping", edgeMappingDocs);


            /*
             * Setting properties
             */

            ODocument propertiesDoc = this.writeConfiguredProperties(currConfiguredEdge.getConfiguredProperties());
            currEdgeInfoDoc.field("properties", propertiesDoc);

            currEdgeDoc.field(edgeClassName, currEdgeInfoDoc);
            edges.add(currEdgeDoc);
        }

        // collecting configured edges
        jsonConfiguration.field("edges", edges);
    }


    private ODocument writeConfiguredProperties(List<OConfiguredProperty> configuredProperties) {
        ODocument propertiesDoc = new ODocument();

        for(OConfiguredProperty currConfiguredProperty: configuredProperties) {
            ODocument currConfiguredPropertyInfoDoc = new ODocument();
            String propertyName = currConfiguredProperty.getPropertyName();
            currConfiguredPropertyInfoDoc.field("include", currConfiguredProperty.isIncludedInMigration());
            currConfiguredPropertyInfoDoc.field("type", currConfiguredProperty.getPropertyType());
            currConfiguredPropertyInfoDoc.field("mandatory", currConfiguredProperty.isMandatory());
            currConfiguredPropertyInfoDoc.field("readOnly", currConfiguredProperty.isReadOnly());
            currConfiguredPropertyInfoDoc.field("notNull", currConfiguredProperty.isNotNull());

            OConfiguredPropertyMapping propertyMapping = currConfiguredProperty.getPropertyMapping();
            if(propertyMapping != null) {
                ODocument propertyMappingDoc = new ODocument();
                propertyMappingDoc.field("source", propertyMapping.getSourceName());
                propertyMappingDoc.field("columnName", propertyMapping.getColumnName());
                propertyMappingDoc.field("type", propertyMapping.getType());
                currConfiguredPropertyInfoDoc.field("mapping", propertyMappingDoc);
            }

            propertiesDoc.field(propertyName, currConfiguredPropertyInfoDoc);
        }

        return propertiesDoc;
    }

    public OConfiguration buildConfigurationFromMapper(OER2GraphMapper mapper) {

        OConfiguration configuration = new OConfiguration();

        /**
         * Building configured vertices starting from Vertices-Type
         */

        Map<String, String> tableName2SourceIdName = buildConfigVerticesFromGraphModel(mapper, configuration);


        /**
         * Building configured edges starting from Edges-Type
         */

        buildConfigEdgesFromGraphModel(mapper, configuration, tableName2SourceIdName);

        return configuration;
    }


    protected Map<String, String> buildConfigVerticesFromGraphModel(OER2GraphMapper mapper, OConfiguration configuration) {

        Map<String,String> tableName2SourceIdName = new HashMap<String,String>();

        // populating tableName2SourceIdName map with all the tables in every case (with aggregation or not)
        for(OVertexType currVertexType: mapper.getVertexType2classMappers().keySet()) {
            OEntity currentEntity = mapper.getEntityByVertexType(currVertexType);
            OSourceDatabaseInfo currSourceDBInfo = currentEntity.getSourceDataseInfo();
            String sourceIdName = currSourceDBInfo.getSourceIdName() + "_" + currentEntity.getName();
            tableName2SourceIdName.put(currentEntity.getName(), sourceIdName);
        }

        List<OConfiguredVertexClass> configuredVertexClasses = new LinkedList<OConfiguredVertexClass>();

        for(OVertexType currVertexType: mapper.getVertexType2classMappers().keySet()) {

            if(this.runningAggregationStrategy && !currVertexType.isFromJoinTable()) {

                OEntity currentEntity = mapper.getEntityByVertexType(currVertexType);
                OSourceDatabaseInfo currSourceDBInfo = currentEntity.getSourceDataseInfo();
                List<OClassMapper> currClassMappers = mapper.getVertexType2classMappers().get(currVertexType);
                if (currClassMappers.size() == 0) {
                    OTeleporterContext.getInstance().
                            getOutputManager().error("Error during the model building: the %s vertex class is not mapped to any vertex class", currVertexType.getName());
                    throw new OTeleporterRuntimeException();
                }
                if (currClassMappers.size() > 1) {
                    OTeleporterContext.getInstance().
                            getOutputManager().error("Error during the model building: the %s vertex class is mapped to more than one vertex classes", currVertexType.getName());
                    throw new OTeleporterRuntimeException();
                }
                OClassMapper currClassMapper = currClassMappers.get(0);
                OConfiguredVertexClass currConfiguredVertexClass = new OConfiguredVertexClass(currVertexType.getName());

                // building vertex mapping info
                OVertexMappingInformation vertexMappingInfo = new OVertexMappingInformation(currConfiguredVertexClass);
                List<OSourceTable> sourceTables = new LinkedList<OSourceTable>();

                String sourceIdName = currSourceDBInfo.getSourceIdName() + "_" + currentEntity.getName();
                OSourceTable sourceTable = new OSourceTable(sourceIdName);
                sourceTable.setDataSource(currSourceDBInfo.getSourceIdName());
                sourceTable.setTableName(currentEntity.getName());
                sourceTables.add(sourceTable);
                vertexMappingInfo.setSourceTables(sourceTables);
                currConfiguredVertexClass.setMapping(vertexMappingInfo);

                // building configured properties
                List<OConfiguredProperty> configuredProperties = new LinkedList<OConfiguredProperty>();
                for (OModelProperty currModelProperty : currVertexType.getProperties()) {
                    OConfiguredProperty currConfiguredProperty = new OConfiguredProperty(currModelProperty.getName());
                    currConfiguredProperty.setIncludedInMigration(true);
                    currConfiguredProperty.setPropertyType(currModelProperty.getOrientdbType());
                    currConfiguredProperty.setMandatory(false);
                    currConfiguredProperty.setReadOnly(false);
                    currConfiguredProperty.setNotNull(false);

                    OConfiguredPropertyMapping propertyMappingInfo = new OConfiguredPropertyMapping(sourceTable.getSourceIdName());
                    String correspondentAttributeName = currClassMapper.getAttributeByProperty(currModelProperty.getName());
                    OAttribute correspondentAttribute = currentEntity.getAttributeByName(correspondentAttributeName);
                    propertyMappingInfo.setColumnName(correspondentAttributeName);
                    propertyMappingInfo.setType(correspondentAttribute.getDataType());
                    currConfiguredProperty.setPropertyMapping(propertyMappingInfo);

                    configuredProperties.add(currConfiguredProperty);
                }
                currConfiguredVertexClass.setConfiguredProperties(configuredProperties);

                // adding configured vertex to the list
                configuredVertexClasses.add(currConfiguredVertexClass);
            }
        }
        configuration.setConfiguredVertices(configuredVertexClasses);
        return tableName2SourceIdName;
    }

    protected void buildConfigEdgesFromGraphModel(OER2GraphMapper mapper, OConfiguration configuration, Map<String, String> tableName2SourceIdName) {
        List<OConfiguredEdgeClass> configuredEdgeClasses = new LinkedList<OConfiguredEdgeClass>();

        for(OEdgeType currEdgeType: mapper.getGraphModel().getEdgesType()) {

            OConfiguredEdgeClass currConfiguredEdgeClass = new OConfiguredEdgeClass(currEdgeType.getName());

            // edge mapping info building
            OEdgeMappingInformation currEdgeMappingInformation = new OEdgeMappingInformation(currConfiguredEdgeClass);
            List<ORelationship> relationships = mapper.getEdgeType2relationships().get(currEdgeType);
            List<OEdgeMappingInformation> edgeMappings = new LinkedList<OEdgeMappingInformation>();
            OAggregatorEdge aggregatorEdge = mapper.getAggregatorEdgeByEdgeTypeName(currEdgeType.getName());

            OVertexType aggregatedVertexType = null;
            OEntity joinTable = null;
            // the current edge type does not represent any join table
            if (aggregatorEdge == null) {

                // building the edge mappings
                for (ORelationship currentRelationship : relationships) {

                    // building the edge mapping info
                    currEdgeMappingInformation.setFromTableName(currentRelationship.getForeignEntity().getName());
                    currEdgeMappingInformation.setToTableName(currentRelationship.getParentEntity().getName());

                    List<String> fromColumns = new LinkedList<String>();
                    for (OAttribute attribute : currentRelationship.getForeignKey().getInvolvedAttributes()) {
                        fromColumns.add(attribute.getName());
                    }
                    List<String> toColumns = new LinkedList<String>();
                    for (OAttribute attribute : currentRelationship.getPrimaryKey().getInvolvedAttributes()) {
                        toColumns.add(attribute.getName());
                    }
                    currEdgeMappingInformation.setFromColumns(fromColumns);
                    currEdgeMappingInformation.setToColumns(toColumns);
                    currEdgeMappingInformation.setDirection(currentRelationship.getDirection());

                    edgeMappings.add(currEdgeMappingInformation);
                }
            }
            // the current edge type represents a join table
            else {

                OVertexType inVertexType = currEdgeType.getInVertexType();
                OVertexType outVertexType = currEdgeType.getOutVertexType();
                OEntity startingTable = mapper.getEntityByVertexType(outVertexType);
                OEntity arrivalTable = mapper.getEntityByVertexType(inVertexType);

                currEdgeMappingInformation.setFromTableName(startingTable.getName());
                currEdgeMappingInformation.setToTableName(arrivalTable.getName());

                List<String> fromColumns = new LinkedList<String>();
                for (OAttribute attribute : startingTable.getPrimaryKey().getInvolvedAttributes()) {
                    fromColumns.add(attribute.getName());
                }
                List<String> toColumns = new LinkedList<String>();
                for (OAttribute attribute : arrivalTable.getPrimaryKey().getInvolvedAttributes()) {
                    toColumns.add(attribute.getName());
                }
                currEdgeMappingInformation.setFromColumns(fromColumns);
                currEdgeMappingInformation.setToColumns(toColumns);
                currEdgeMappingInformation.setDirection("direct");

                // the edgeType corresponds to a join table, so the join table mapping will be built
                aggregatedVertexType = mapper.getJoinVertexTypeByAggregatorEdge(aggregatorEdge.getEdgeType().getName());
                joinTable = mapper.getEntityByVertexType(aggregatedVertexType);
                OAggregatedJoinTableMapping joinTableMappingInfo = new OAggregatedJoinTableMapping(joinTable.getName());

                Iterator<ORelationship> outRelationshipsIterator = joinTable.getOutRelationships().iterator();
                ORelationship currRelationship = outRelationshipsIterator.next();
                fromColumns = new LinkedList<String>();
                for (OAttribute attribute : currRelationship.getForeignKey().getInvolvedAttributes()) {
                    fromColumns.add(attribute.getName());
                }
                currRelationship = outRelationshipsIterator.next();
                toColumns = new LinkedList<String>();
                for (OAttribute attribute : currRelationship.getForeignKey().getInvolvedAttributes()) {
                    toColumns.add(attribute.getName());
                }
                joinTableMappingInfo.setFromColumns(fromColumns);
                joinTableMappingInfo.setToColumns(toColumns);

                currEdgeMappingInformation.setRepresentedJoinTableMapping(joinTableMappingInfo);
                edgeMappings.add(currEdgeMappingInformation);
            }
            currConfiguredEdgeClass.setMappings(edgeMappings);

            // building configured properties
            List<OConfiguredProperty> configuredProperties = new LinkedList<OConfiguredProperty>();
            for(OModelProperty currModelProperty: currEdgeType.getProperties()) {
                OConfiguredProperty currConfiguredProperty = new OConfiguredProperty(currModelProperty.getName());
                currConfiguredProperty.setIncludedInMigration(true);
                currConfiguredProperty.setPropertyType(currModelProperty.getOrientdbType());
                currConfiguredProperty.setMandatory(false);
                currConfiguredProperty.setReadOnly(false);
                currConfiguredProperty.setNotNull(false);

                // the property mapping is present iff it comes from a column of an aggregated join table
                // => the property mapping is present iff there was a join table aggregation
                if(joinTable != null) {
                    OConfiguredPropertyMapping propertyMappingInfo = new OConfiguredPropertyMapping(tableName2SourceIdName.get(joinTable.getName()));
                    OClassMapper currClassMapper = mapper.getClassMappersByEntity(joinTable).get(0);  // join table is mapped with just a vertex class
                    String correspondentAttributeName = currClassMapper.getAttributeByProperty(currModelProperty.getName());
                    OAttribute correspondentAttribute = joinTable.getAttributeByName(correspondentAttributeName);
                    propertyMappingInfo.setColumnName(correspondentAttributeName);
                    propertyMappingInfo.setType(correspondentAttribute.getDataType());
                    currConfiguredProperty.setPropertyMapping(propertyMappingInfo);
                }
                configuredProperties.add(currConfiguredProperty);
            }
            currConfiguredEdgeClass.setConfiguredProperties(configuredProperties);

            // adding configured edge to the list
            configuredEdgeClasses.add(currConfiguredEdgeClass);
        }
        configuration.setConfiguredEdges(configuredEdgeClasses);
    }
}