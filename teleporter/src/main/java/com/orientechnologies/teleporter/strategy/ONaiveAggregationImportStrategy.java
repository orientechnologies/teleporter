/*
 * Copyright 2015 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.teleporter.strategy;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.factory.ODataTypeHandlerFactory;
import com.orientechnologies.teleporter.factory.OMapperFactory;
import com.orientechnologies.teleporter.importengine.ODBQueryEngine;
import com.orientechnologies.teleporter.importengine.OGraphDBCommandEngine;
import com.orientechnologies.teleporter.mapper.OAggregatorEdge;
import com.orientechnologies.teleporter.mapper.OER2GraphMapper;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;
import com.orientechnologies.teleporter.model.dbschema.ORelationship;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OGraphModel;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.persistence.handler.ODriverDataTypeHandler;
import com.orientechnologies.teleporter.persistence.util.OQueryResult;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * A strategy that performs a "naive" import of the data source. The data source schema is
 * translated semi-directly in a correspondent and coherent graph model using an aggregation 
 * policy on the junction tables of dimension equals to 2.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ONaiveAggregationImportStrategy extends ONaiveImportStrategy {	

	public ONaiveAggregationImportStrategy() {}

	@Override
	public OSource2GraphMapper createSchemaMapper(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, ONameResolver nameResolver,
			List<String> includedTables, List<String> excludedTables, OTeleporterContext context) {

		OMapperFactory mapperFactory = new OMapperFactory();
		OSource2GraphMapper mapper = mapperFactory.buildMapper(chosenMapper, driver, uri, username, password, xmlPath, includedTables, excludedTables, context);

		// DataBase schema building
		mapper.buildSourceSchema(context);
		context.getOutputManager().info("");
		context.getOutputManager().debug("%s\n", ((OER2GraphMapper)mapper).getDataBaseSchema().toString());

		// Graph model building
		mapper.buildGraphModel(nameResolver, context);
		context.getOutputManager().info("");
		context.getOutputManager().debug("%s\n", ((OER2GraphMapper)mapper).getGraphModel().toString());

		// Many-to-Many aggregation
		((OER2GraphMapper)mapper).JoinTableDim2Aggregation(context);
		context.getOutputManager().debug("'Junction-Entity' aggregation complete.\n");
		context.getOutputManager().debug("%s\n", ((OER2GraphMapper)mapper).getGraphModel().toString());


		// Saving schema on Orient
		ODataTypeHandlerFactory factory = new ODataTypeHandlerFactory();
		ODriverDataTypeHandler handler = factory.buildDataTypeHandler(driver, context);
		OGraphModelWriter graphModelWriter = new OGraphModelWriter();  
		OGraphModel graphModel = ((OER2GraphMapper)mapper).getGraphModel();
		boolean success = graphModelWriter.writeModelOnOrient(graphModel, handler, outOrientGraphUri, context);
		if(!success) {
			context.getOutputManager().error("Writing not complete. Something's gone wrong.\n");
			System.exit(0);
		}
		context.getOutputManager().debug("OrientDB Schema writing complete.");
		context.getOutputManager().info("");


		return mapper;
	}


	@Override
	public void executeImport(String driver, String uri, String username, String password, String outOrientGraphUri, OSource2GraphMapper genericMapper,  OTeleporterContext context) {

		try {

			OTeleporterStatistics statistics = context.getStatistics();
			statistics.startWork4Time = new Date();
			statistics.runningStepNumber = 4;

			OER2GraphMapper mapper = (OER2GraphMapper) genericMapper;
			ODBQueryEngine dbQueryEngine = new ODBQueryEngine(driver, uri, username, password);    
			OGraphDBCommandEngine graphDBCommandEngine = new OGraphDBCommandEngine(outOrientGraphUri);

			OVertexType currentOutVertexType = null;  
			OVertexType currentInVertexType = null;  
			OrientVertex currentOutVertex = null;
			OEdgeType edgeType = null;

			// Importing from Entities belonging to hierarchical bags
			for(OHierarchicalBag bag: mapper.getDataBaseSchema().getHierarchicalBags()) {

				switch(bag.getInheritancePattern()) {

				case "table-per-hierarchy": super.tablePerHierarchyImport(bag, mapper, dbQueryEngine, graphDBCommandEngine, context);
				break;

				case "table-per-type": super.tablePerTypeImport(bag, mapper, dbQueryEngine, graphDBCommandEngine, context);
				break;

				case "table-per-concrete-type": super.tablePerConcreteTypeImport(bag, mapper, dbQueryEngine, graphDBCommandEngine, context);
				break;

				}
			}

			OQueryResult queryResult = null;
			ResultSet records = null;

			// Importing from Entities NOT belonging to hierarchical bags and NOT corresponding to join tables
			for(OEntity entity: mapper.getDataBaseSchema().getEntities()) {

				if(!entity.isJoinEntityDim2() && entity.getHierarchicalBag() == null) {

					// for each entity in dbSchema all records are retrieved
					queryResult = dbQueryEngine.getRecordsByEntity(entity.getName(), context);
					records = queryResult.getResult();
					ResultSet currentRecord = null;

					currentOutVertexType = mapper.getEntity2vertexType().get(entity);

					// each record is imported as vertex in the orient graph
					while(records.next()) {
						// upsert of the vertex
						currentRecord = records;
						currentOutVertex = (OrientVertex) graphDBCommandEngine.upsertVisitedVertex(currentRecord, currentOutVertexType, null, context);

						// for each attribute of the entity belonging to the primary key, correspondent relationship is
						// built as edge and for the referenced record a vertex is built (only id)
						for(ORelationship currentRelation: entity.getRelationships()) {
							currentInVertexType = mapper.getVertexTypeByName(context.getNameResolver().resolveVertexName(currentRelation.getParentEntityName()));
							edgeType = mapper.getRelationship2edgeType().get(currentRelation);
							graphDBCommandEngine.upsertReachedVertexWithEdge(currentRecord, currentRelation, currentOutVertex, currentInVertexType, edgeType.getName(), context);
						}   

						// Statistics updated
						statistics.importedRecords++;
					}
					// closing resultset, connection and statement
					queryResult.closeAll(context);
				}
			}

			// Importing from Entities NOT belonging to hierarchical bags and corresponding to join tables
			for(OEntity entity: mapper.getDataBaseSchema().getEntities()) {

				if(entity.isJoinEntityDim2() && entity.getHierarchicalBag() == null) {

					// for each entity in dbSchema all records are retrieved
					queryResult = dbQueryEngine.getRecordsByEntity(entity.getName(), context);
					records = queryResult.getResult();
					ResultSet currentRecord = null;

					OAggregatorEdge aggregatorEdge = mapper.getJoinVertex2aggregatorEdges().get(context.getNameResolver().resolveVertexName(entity.getName()));

					// each record of the join table used to add an edge
					while(records.next()) {
						currentRecord = records;
						graphDBCommandEngine.upsertAggregatorEdge(currentRecord, entity, aggregatorEdge, context);

						// Statistics updated
						statistics.importedRecords++;
					}
					// closing resultset, connection and statement
					queryResult.closeAll(context);
				}
			}

			statistics.notifyListeners();
			statistics.runningStepNumber = -1;
			context.getOutputManager().info("");

		} catch(Exception e){
			if(e.getMessage() != null)
				context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
			else
				context.getOutputManager().error(e.getClass().getName());

			Writer writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));
			context.getOutputManager().debug(writer.toString());
		}
	}

}
