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

import java.util.*;

/**
 * It parses the jsonConfiguration JSON and builds a OConfiguration object to handle all the information
 * in a easy way.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 *
 */

public class OConfigurationParser {

    public OConfiguration buildConfigurationFromJSON(ODocument jsonConfiguration, OTeleporterContext context) {

        OConfiguration configuration = new OConfiguration();

        // parsing vertices' jsonConfiguration
        this.buildConfiguredVertices(jsonConfiguration, configuration, context);

        // parsing edges' jsonConfiguration
        this.buildConfiguredEdges(jsonConfiguration, configuration, context);

        return configuration;
    }


    private void buildConfiguredVertices(ODocument jsonConfiguration, OConfiguration configuration, OTeleporterContext context) {

        List<ODocument> verticesDoc = jsonConfiguration.field("vertices");
        List<OConfiguredVertexClass> configuredVertices = new LinkedList<OConfiguredVertexClass>();

        if (verticesDoc == null) {
            context.getOutputManager().error("Configuration error: 'vertices' field not found.");
            throw new OTeleporterRuntimeException();
        }

        // building vertices
        for (ODocument currentVertexDoc : verticesDoc) {

            String configuredVertexClassName = currentVertexDoc.field("name");
            OConfiguredVertexClass currentConfiguredVertex = new OConfiguredVertexClass(configuredVertexClassName);
            OVertexMappingInformation currentMapping = new OVertexMappingInformation(currentConfiguredVertex);
            ODocument mappingDoc = currentVertexDoc.field("mapping");

            if(mappingDoc == null) {
                context.getOutputManager().error("Configuration error: 'mapping' field not found in the '%s' edge-type definition.",  configuredVertexClassName);
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
            List<OSourceTable> sourceTables = new LinkedList<OSourceTable>();

            Map<String, String> sourceId2tableName = new HashMap<String, String>();

            int i = 0;
            for (ODocument sourceTable : sourceTableDocs) {
                String sourceIdName = sourceTable.field("name");
                String sourceTableName = sourceTable.field("tableName");
                String dataSource = sourceTable.field("dataSource");
                if(sourceIdName == null) {
                    context.getOutputManager().error("Configuration error: 'name' field not found in the '%s' vertex-class mapping with the source table.",  configuredVertexClassName);
                    throw new OTeleporterRuntimeException();
                }
                if(sourceTableName == null) {
                    context.getOutputManager().error("Configuration error: 'tableName' field not found in the '%s' vertex-class mapping with the source table.",  configuredVertexClassName);
                    throw new OTeleporterRuntimeException();
                }
                sourceId2tableName.put(sourceIdName, sourceTableName);

                List<String> aggregationColumns = sourceTable.field("aggregationColumns");

                OSourceTable currentSourceTable = new OSourceTable(sourceIdName);
                currentSourceTable.setDataSource(dataSource);
                currentSourceTable.setTableName(sourceTableName);
                currentSourceTable.setAggregationColumns(aggregationColumns);
                sourceTables.add(currentSourceTable);

                i++;
            }
            currentMapping.setSourceTables(sourceTables);
            currentConfiguredVertex.setMapping(currentMapping);

            /**
             *  extract and set configured properties
             */
            List<OConfiguredProperty> configuredProperties = this.extractProperties(currentVertexDoc, context);
            currentConfiguredVertex.setConfiguredProperties(configuredProperties);
            configuredVertices.add(currentConfiguredVertex);
        }

        configuration.setConfiguredVertices(configuredVertices);
    }

    private void buildConfiguredEdges(ODocument jsonConfiguration, OConfiguration configuration, OTeleporterContext context) {

        List<ODocument> edgesDoc = jsonConfiguration.field("edges");
        List<OConfiguredEdgeClass> configuredEdges = new LinkedList<OConfiguredEdgeClass>();

        if(edgesDoc == null) {
            context.getOutputManager().error("Configuration error: 'edges' field not found.");
            throw new OTeleporterRuntimeException();
        }

        // building edges
        for(ODocument currentEdgeDoc: edgesDoc) {

            String[] edgeFields = currentEdgeDoc.fieldNames();
            if(edgeFields.length != 1) {
                context.getOutputManager().error("Configuration error: wrong edge definition.");
            }
            String configuredEdgeClassName = edgeFields[0];
            OConfiguredEdgeClass currentConfiguredEdge = new OConfiguredEdgeClass(configuredEdgeClassName);
            OEdgeMappingInformation currentMapping = new OEdgeMappingInformation(currentConfiguredEdge);
            OAggregatedJoinTableMapping joinTableMapping = null;

            ODocument currentEdgeInfo = currentEdgeDoc.field(configuredEdgeClassName);
            ODocument mappingDoc = currentEdgeInfo.field("mapping");

            // building configured edges
            if(mappingDoc == null) {
                context.getOutputManager().error("Configuration error: 'mapping' field not found in the '%s' edge-type definition.",  configuredEdgeClassName);
                throw new OTeleporterRuntimeException();
            }

            String currentForeignEntityName = mappingDoc.field("fromTable");
            String currentParentEntityName = mappingDoc.field("toTable");
            List<String> fromColumns = mappingDoc.field("fromColumns");
            List<String> toColumns = mappingDoc.field("toColumns");

            ODocument joinTableDoc = mappingDoc.field("joinTable");

            // jsonConfiguration errors managing (draconian approach)
            if(currentForeignEntityName == null) {
                context.getOutputManager().error("Configuration error: 'fromTable' field not found in the '%s' edge-type definition.",  configuredEdgeClassName);
                throw new OTeleporterRuntimeException();
            }
            if(currentParentEntityName == null) {
                context.getOutputManager().error("Configuration error: 'toTable' field not found in the '%s' edge-type definition.",  configuredEdgeClassName);
                throw new OTeleporterRuntimeException();
            }
            if(fromColumns == null) {
                context.getOutputManager().error("Configuration error: 'fromColumns' field not found in the '%s' edge-type definition.",  configuredEdgeClassName);
                throw new OTeleporterRuntimeException();
            }
            if(toColumns == null) {
                context.getOutputManager().error("Configuration error: 'toColumns' field not found in the '%s' edge-type definition.",  configuredEdgeClassName);
                throw new OTeleporterRuntimeException();
            }

            String direction = mappingDoc.field("direction");

            if(direction != null && !(direction.equals("direct") || direction.equals("inverse"))) {
                context.getOutputManager().error("Configuration error: direction for the edge %s cannot be '%s'. Allowed values: 'direct' or 'inverse' \n", configuredEdgeClassName, direction);
                throw new OTeleporterRuntimeException();
            }

            boolean foreignEntityIsJoinTableToAggregate = false;

            if(joinTableDoc != null) {

                String joinTableName = joinTableDoc.field("tableName");

                if(joinTableName == null) {
                    context.getOutputManager().error("Configuration error: 'tableName' field not found in the join table mapping with the '%s' edge-type.",  configuredEdgeClassName);
                    throw new OTeleporterRuntimeException();
                }

                joinTableMapping = new OAggregatedJoinTableMapping(joinTableName);
                foreignEntityIsJoinTableToAggregate = true;

                if(context.getExecutionStrategy().equals("naive-aggregate")) {
                    // strategy is aggregated
                    List<String> joinTableFromColumns = joinTableDoc.field("fromColumns");
                    List<String> joinTableToColumns = joinTableDoc.field("toColumns");

                    if(joinTableFromColumns == null) {
                        context.getOutputManager().error("Configuration error: 'fromColumns' field not found in the join table mapping with the '%s' edge-type.",  configuredEdgeClassName);
                        throw new OTeleporterRuntimeException();
                    }
                    if(joinTableToColumns == null) {
                        context.getOutputManager().error("Configuration error: 'toColumns' field not found in the join table mapping with the '%s' edge-type.",  configuredEdgeClassName);
                        throw new OTeleporterRuntimeException();
                    }

                    joinTableMapping.setFromColumns(joinTableFromColumns);
                    joinTableMapping.setToColumns(joinTableToColumns);
                }
                else if(context.getExecutionStrategy().equals("naive")) {
                    context.getOutputManager().error("Configuration not compliant with the chosen strategy: you cannot perform the aggregation declared in the jsonConfiguration for the "
                            + "join table %s while executing migration with a not-aggregating strategy. Thus no aggregation will be performed.\n", joinTableName);
                    throw new OTeleporterRuntimeException();
                }
            }

            // Updating edge's jsonConfiguration
            currentMapping.setFromTableName(currentForeignEntityName);
            currentMapping.setToTableName(currentParentEntityName);
            currentMapping.setFromColumns(fromColumns);
            currentMapping.setToColumns(toColumns);
            currentMapping.setDirection(direction);
            currentMapping.setRepresentedJoinTableMapping(joinTableMapping);
            ((OConfiguredEdgeClass)currentConfiguredEdge).setMapping(currentMapping);

            // extract and set configured properties
            List<OConfiguredProperty> configuredProperties = this.extractProperties(currentEdgeInfo, context);
            currentConfiguredEdge.setConfiguredProperties(configuredProperties);
            configuredEdges.add(currentConfiguredEdge);
        }
        configuration.setConfiguredEdges(configuredEdges);
    }

    private List<OConfiguredProperty> extractProperties(ODocument currentElementInfo, OTeleporterContext context) {

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
                if(propertyType == null) {
                    context.getStatistics().warningMessages.add("Configuration ERROR: the property " + propertyName + " will not added to the correspondent Class because the type is badly defined or not defined at all.");
                    continue;
                }

                Boolean mandatory = currentElementPropertyDoc.field("mandatory");
                Boolean readOnly = currentElementPropertyDoc.field("readOnly");
                Boolean notNull = currentElementPropertyDoc.field("notNull");

                // extracting mapping info (optional: may be null if the property is defined from scratch (only schema definition))
                ODocument propertyMappingInfo = currentElementPropertyDoc.field("mapping");
                OConfiguredPropertyMapping propertyMapping = null;
                if(propertyMappingInfo != null) {
                    String source = propertyMappingInfo.field("source");
                    String columnName = propertyMappingInfo.field("columnName");
                    String type = propertyMappingInfo.field("type");
                    propertyMapping = new OConfiguredPropertyMapping(source);
                    propertyMapping.setColumnName(columnName);
                    propertyMapping.setType(type);
                }

                OConfiguredProperty currentConfiguredProperty = new OConfiguredProperty(propertyName);
                if(isIncludedInMigration != null) {
                    currentConfiguredProperty.setIncludedInMigration(isIncludedInMigration);
                }
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
}
