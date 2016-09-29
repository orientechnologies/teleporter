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

package com.orientechnologies.teleporter.test.rdbms.configuration.handler;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.*;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.util.OFileManager;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class OConfigurationHandlerTest {

    private final String config1 = "src/test/resources/configuration-mapping/full-configuration-mapping.json";
    private final String config2 = "src/test/resources/configuration-mapping/joint-table-relationships-mapping-direct-edges.json";
    private OTeleporterContext context;
    OConfigurationHandler configurationHandler;

    @Before
    public void init() {
        this.context = new OTeleporterContext();
        this.context.setOutputManager(new OOutputStreamManager(0));
        this.context.setExecutionStrategy("naive-aggregate");
        this.configurationHandler = new OConfigurationHandler();
    }

    @Test
    public void test1() {

        ODocument inputConfigurationDoc = null;
        try {
            inputConfigurationDoc = OFileManager.buildJsonFromFile(this.config1);
        }catch(IOException e) {
            e.printStackTrace();
            fail();
        }

        OConfiguration configuration = this.configurationHandler.buildConfigurationFromJSON(inputConfigurationDoc, this.context);


        /**
         * Checking configured vertices
         */

        assertEquals(2, configuration.getConfiguredVertices().size());

        // person vertex class

        OConfiguredVertexClass personVertexClass = configuration.getConfiguredVertices().get(0);
        OVertexMappingInformation personMapping =  personVertexClass.getMapping();

        assertEquals("Person", personVertexClass.getName());
        assertNotNull(personMapping);
        assertEquals(personVertexClass, personMapping.getBelongingVertex());
        assertEquals("equality", personMapping.getAggregationFunction());
        assertNotNull(personMapping.getSourceTables());
        assertEquals(2, personMapping.getSourceTables().size());

        OSourceTable personSourceTable = personMapping.getSourceTables().get(0);
        OSourceTable vatProfileSourceTable = personMapping.getSourceTables().get(1);

        assertNotNull(personSourceTable);
        assertNotNull(vatProfileSourceTable);
        assertEquals("sourceTable1", personSourceTable.getSourceIdName());
        assertEquals("mysql", personSourceTable.getDataSource());
        assertEquals("PERSON", personSourceTable.getTableName());
        assertNotNull(personSourceTable.getAggregationColumns());
        assertEquals(1, personSourceTable.getAggregationColumns().size());
        assertEquals("ID", personSourceTable.getAggregationColumns().get(0));
        assertEquals("sourceTable2", vatProfileSourceTable.getSourceIdName());
        assertEquals("mysql", vatProfileSourceTable.getDataSource());
        assertEquals("VAT_PROFILE", vatProfileSourceTable.getTableName());
        assertNotNull(vatProfileSourceTable.getAggregationColumns());
        assertEquals(1, vatProfileSourceTable.getAggregationColumns().size());
        assertEquals("ID", vatProfileSourceTable.getAggregationColumns().get(0));

        List<OConfiguredProperty> properties = personVertexClass.getConfiguredProperties();
        assertNotNull(properties);
        assertEquals(7,  properties.size());

        OConfiguredProperty currentConfiguredProperty = properties.get(0);
        OConfiguredPropertyMapping currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("extKey1", currentConfiguredProperty.getPropertyName());
        assertEquals("string", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(false, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(false, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
        assertEquals("ID", currentPropertyMapping.getColumnName());
        assertEquals("VARCHAR", currentPropertyMapping.getType());

        currentConfiguredProperty = properties.get(1);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("firstName", currentConfiguredProperty.getPropertyName());
        assertEquals("string", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(true, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(true, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
        assertEquals("NAME", currentPropertyMapping.getColumnName());
        assertEquals("VARCHAR", currentPropertyMapping.getType());

        currentConfiguredProperty = properties.get(2);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("lastName", currentConfiguredProperty.getPropertyName());
        assertEquals("string", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(true, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(true, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
        assertEquals("SURNAME", currentPropertyMapping.getColumnName());
        assertEquals("VARCHAR", currentPropertyMapping.getType());

        currentConfiguredProperty = properties.get(3);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("depId", currentConfiguredProperty.getPropertyName());
        assertEquals("string", currentConfiguredProperty.getPropertyType());
        assertEquals(false, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(false, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(false, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
        assertEquals("DEP_ID", currentPropertyMapping.getColumnName());
        assertEquals("VARCHAR", currentPropertyMapping.getType());

        currentConfiguredProperty = properties.get(4);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("extKey2", currentConfiguredProperty.getPropertyName());
        assertEquals("string", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(false, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(false, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable2", currentPropertyMapping.getSourceName());
        assertEquals("ID", currentPropertyMapping.getColumnName());
        assertEquals("VARCHAR", currentPropertyMapping.getType());

        currentConfiguredProperty = properties.get(5);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("VAT", currentConfiguredProperty.getPropertyName());
        assertEquals("string", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(true, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(true, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable2", currentPropertyMapping.getSourceName());
        assertEquals("VAT", currentPropertyMapping.getColumnName());
        assertEquals("VARCHAR", currentPropertyMapping.getType());

        currentConfiguredProperty = properties.get(6);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("updatedOn", currentConfiguredProperty.getPropertyName());
        assertEquals("date", currentConfiguredProperty.getPropertyType());
        assertEquals(false, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(true, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(true, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable2", currentPropertyMapping.getSourceName());
        assertEquals("UPDATED_ON", currentPropertyMapping.getColumnName());
        assertEquals("DATE", currentPropertyMapping.getType());


        // department vertex class

        OConfiguredVertexClass departmentVertexClass = configuration.getConfiguredVertices().get(1);
        OVertexMappingInformation departmentMapping =  departmentVertexClass.getMapping();

        assertEquals("Department", departmentVertexClass.getName());
        assertNotNull(departmentMapping);
        assertEquals(departmentVertexClass, departmentMapping.getBelongingVertex());
        assertNull(departmentMapping.getAggregationFunction());
        assertNotNull(departmentMapping.getSourceTables());
        assertEquals(1, departmentMapping.getSourceTables().size());

        OSourceTable departmentSourceTable = departmentMapping.getSourceTables().get(0);

        assertNotNull(departmentSourceTable);
        assertEquals("sourceTable1", departmentSourceTable.getSourceIdName());
        assertEquals("mysql", departmentSourceTable.getDataSource());
        assertEquals("DEPARTMENT", departmentSourceTable.getTableName());
        assertNull(departmentSourceTable.getAggregationColumns());

        properties = departmentVertexClass.getConfiguredProperties();
        assertNotNull(properties);
        assertEquals(4,  properties.size());

        currentConfiguredProperty = properties.get(0);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("id", currentConfiguredProperty.getPropertyName());
        assertEquals("string", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(false, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(false, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
        assertEquals("ID", currentPropertyMapping.getColumnName());
        assertEquals("VARCHAR", currentPropertyMapping.getType());

        currentConfiguredProperty = properties.get(1);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("departmentName", currentConfiguredProperty.getPropertyName());
        assertEquals("string", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(true, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(true, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
        assertEquals("NAME", currentPropertyMapping.getColumnName());
        assertEquals("VARCHAR", currentPropertyMapping.getType());

        currentConfiguredProperty = properties.get(2);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("location", currentConfiguredProperty.getPropertyName());
        assertEquals("string", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(true, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(true, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
        assertEquals("LOCATION", currentPropertyMapping.getColumnName());
        assertEquals("VARCHAR", currentPropertyMapping.getType());

        currentConfiguredProperty = properties.get(3);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNotNull(currentPropertyMapping);
        assertEquals("updatedOn", currentConfiguredProperty.getPropertyName());
        assertEquals("date", currentConfiguredProperty.getPropertyType());
        assertEquals(false, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(true, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(true, currentConfiguredProperty.isNotNull());
        assertEquals("sourceTable1", currentPropertyMapping.getSourceName());
        assertEquals("UPDATED_ON", currentPropertyMapping.getColumnName());
        assertEquals("DATE", currentPropertyMapping.getType());


        /**
         * Checking configured edges
         */

        assertEquals(1, configuration.getConfiguredEdges().size());

        OConfiguredEdgeClass worksAtEdgeClass = configuration.getConfiguredEdges().get(0);
        assertEquals("WorksAt", worksAtEdgeClass.getName());

        List<OEdgeMappingInformation> worksAtMappings =  worksAtEdgeClass.getMappings();
        assertEquals(1, worksAtMappings.size());
        OEdgeMappingInformation worksAtMapping1 = worksAtMappings.get(0);
        assertNotNull(worksAtMapping1);
        assertEquals(worksAtEdgeClass, worksAtMapping1.getBelongingEdge());
        assertEquals("PERSON", worksAtMapping1.getFromTableName());
        assertEquals(1, worksAtMapping1.getFromColumns().size());
        assertEquals("DEP_ID", worksAtMapping1.getFromColumns().get(0));
        assertEquals("DEPARTMENT", worksAtMapping1.getToTableName());
        assertEquals(1, worksAtMapping1.getToColumns().size());
        assertEquals("ID", worksAtMapping1.getToColumns().get(0));
        assertEquals("direct", worksAtMapping1.getDirection());
        assertNull(worksAtMapping1.getRepresentedJoinTableMapping());

        properties = worksAtEdgeClass.getConfiguredProperties();
        assertNotNull(properties);
        assertEquals(1,  properties.size());

        currentConfiguredProperty = properties.get(0);
        currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNull(currentPropertyMapping);
        assertEquals("since", currentConfiguredProperty.getPropertyName());
        assertEquals("date", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(true, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(false, currentConfiguredProperty.isNotNull());


        /**
         * 1. Writing the configuration on a second JSON document through the configurationHandler.
         * 2. Checking that the original JSON configuration and the final just written configuration are equal.
         */
        ODocument writtenJsonConfiguration = this.configurationHandler.buildJSONFromConfiguration(configuration, context);
        assertEquals(inputConfigurationDoc.toJSON(""), writtenJsonConfiguration.toJSON(""));
    }


    @Test
    public void test2() {

        ODocument inputConfigurationDoc = null;
        try {
            inputConfigurationDoc = OFileManager.buildJsonFromFile(this.config2);
        }catch(IOException e) {
            e.printStackTrace();
            fail();
        }

        OConfiguration configuration = this.configurationHandler.buildConfigurationFromJSON(inputConfigurationDoc, this.context);


        /**
         * Checking configured vertices
         */

        assertEquals(0, configuration.getConfiguredVertices().size());


        /**
         * Checking configured edges
         */


        assertEquals(1, configuration.getConfiguredEdges().size());

        OConfiguredEdgeClass performsEdgeClass = configuration.getConfiguredEdges().get(0);
        assertEquals("Performs", performsEdgeClass.getName());

        List<OEdgeMappingInformation> performsMappings =  performsEdgeClass.getMappings();
        assertEquals(1, performsMappings.size());
        OEdgeMappingInformation performsMapping1 = performsMappings.get(0);
        assertNotNull(performsMapping1);
        assertEquals(performsEdgeClass, performsMapping1.getBelongingEdge());
        assertEquals("ACTOR", performsMapping1.getFromTableName());
        assertEquals(1, performsMapping1.getFromColumns().size());
        assertEquals("ID", performsMapping1.getFromColumns().get(0));
        assertEquals("FILM", performsMapping1.getToTableName());
        assertEquals(1, performsMapping1.getToColumns().size());
        assertEquals("ID", performsMapping1.getToColumns().get(0));
        assertEquals("direct", performsMapping1.getDirection());
        assertNotNull(performsMapping1.getRepresentedJoinTableMapping());
        assertEquals("ACTOR_FILM", performsMapping1.getRepresentedJoinTableMapping().getTableName());
        assertEquals(1, performsMapping1.getRepresentedJoinTableMapping().getFromColumns().size());
        assertEquals("ACTOR_ID", performsMapping1.getRepresentedJoinTableMapping().getFromColumns().get(0));
        assertEquals(1, performsMapping1.getRepresentedJoinTableMapping().getToColumns().size());
        assertEquals("FILM_ID", performsMapping1.getRepresentedJoinTableMapping().getToColumns().get(0));

        List<OConfiguredProperty> properties = performsEdgeClass.getConfiguredProperties();
        assertNotNull(properties);
        assertEquals(1,  properties.size());

        OConfiguredProperty currentConfiguredProperty = properties.get(0);
        OConfiguredPropertyMapping currentPropertyMapping = currentConfiguredProperty.getPropertyMapping();
        assertNotNull(currentConfiguredProperty);
        assertNull(currentPropertyMapping);
        assertEquals("year", currentConfiguredProperty.getPropertyName());
        assertEquals("DATE", currentConfiguredProperty.getPropertyType());
        assertEquals(true, currentConfiguredProperty.isIncludedInMigration());
        assertEquals(true, currentConfiguredProperty.isMandatory());
        assertEquals(false, currentConfiguredProperty.isReadOnly());
        assertEquals(false, currentConfiguredProperty.isNotNull());

        /**
         * 1. Writing the configuration on a second JSON document through the configurationHandler.
         * 2. Checking that the original JSON configuration and the final just written configuration are equal.
         */
        ODocument writtenJsonConfiguration = this.configurationHandler.buildJSONFromConfiguration(configuration, context);
        assertEquals(inputConfigurationDoc.toJSON(""), writtenJsonConfiguration.toJSON(""));
    }

}
