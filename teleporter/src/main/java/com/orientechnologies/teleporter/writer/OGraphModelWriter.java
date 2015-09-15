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

package com.orientechnologies.teleporter.writer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.model.graphmodel.OEdgeType;
import com.orientechnologies.teleporter.model.graphmodel.OElementType;
import com.orientechnologies.teleporter.model.graphmodel.OGraphModel;
import com.orientechnologies.teleporter.model.graphmodel.OModelProperty;
import com.orientechnologies.teleporter.model.graphmodel.OVertexType;
import com.orientechnologies.teleporter.persistence.handler.ODriverDataTypeHandler;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientElementType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Writer that has the responsibility to write the model of the destination Orient Graph
 * on OrientDB as an OrientDB Schema.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OGraphModelWriter {


	public OGraphModelWriter() {}


	public boolean writeModelOnOrient(OGraphModel graphModel, ODriverDataTypeHandler handler, String outOrientGraphUri, OTeleporterContext context) {
		boolean success = false;

		OrientGraphNoTx orientGraph = new OrientGraphNoTx(outOrientGraphUri);
		OTeleporterStatistics statistics = context.getStatistics();
		statistics.startWork3Time = new Date();
		statistics.runningStepNumber = 3;

		if(!this.inheritanceChangesPresent(graphModel, orientGraph)) {

			try {

				/*
				 * Writing vertex-type
				 */

				context.getOutputManager().debug("Writing vertex-types on OrientDB Schema...");
				int numberOfVertices = graphModel.getVerticesType().size();
				statistics.totalNumberOfVertexType = numberOfVertices;

				OrientVertexType newVertexType;
				String statement;
				OCommandSQL sqlCommand;
				OType type;
				Iterator<OModelProperty> it = null;

				int iteration = 1;
				for(OVertexType currentVertexType: graphModel.getVerticesType()) {
					context.getOutputManager().debug("Writing '%s' vertex-type  (%s/%s)...", currentVertexType.getName(), iteration, numberOfVertices);

					// check if vertex type is already present in the orient schema
					newVertexType = orientGraph.getVertexType(currentVertexType.getName());

					if(newVertexType == null) {

						// inheritance case
						if(currentVertexType.getParentType() != null)
							newVertexType = orientGraph.createVertexType(currentVertexType.getName(), currentVertexType.getParentType().getName());
						else
							newVertexType = orientGraph.createVertexType(currentVertexType.getName());

						OModelProperty currentProperty = null;
						it = currentVertexType.getProperties().iterator();
						while(it.hasNext()) {
							currentProperty = it.next();
							type = handler.resolveType(currentProperty.getPropertyType().toLowerCase(Locale.ENGLISH), context);
							if(type != null) {
								String propertyName = currentProperty.getName();
								newVertexType.createProperty(propertyName, type);
							}
							else {
								it.remove();
								statistics.warningMessages.add(currentProperty.getPropertyType() + " type is not supported, the correspondent property will be dropped.");
							}
						}
						context.getOutputManager().debug("Vertex-type '%s' wrote.\n", currentVertexType.getName());
					}
					else  {
						boolean updated = this.checkAndUpdateClass(orientGraph, currentVertexType, handler, context);

						if(updated) {
							context.getOutputManager().debug("Vertex-type '%s' updated.\n", currentVertexType.getName());
						}
						else {
							context.getOutputManager().debug("Vertex-type '%s' already present in the Orient schema.\n", currentVertexType.getName());
						}
					}

					iteration++;
					statistics.wroteVertexType++;
				}

				/*
				 * Writing edge-type
				 */

				context.getOutputManager().debug("Writing edge-types on OrientDB Schema...");
				int numberOfEdges = graphModel.getEdgesType().size();
				statistics.totalNumberOfEdgeType = numberOfEdges;

				OrientEdgeType newEdgeType;

				iteration = 1;
				for(OEdgeType currentEdgeType: graphModel.getEdgesType()) {
					context.getOutputManager().debug("Writing '%s' edge-type  (%s/%s)...", currentEdgeType.getName(), iteration, numberOfEdges);

					// check if edge type is already present in the orient schema
					newEdgeType = orientGraph.getEdgeType(currentEdgeType.getName());

					if(newEdgeType == null) {
						newEdgeType = orientGraph.createEdgeType(currentEdgeType.getName());
						OModelProperty currentProperty = null;
						it = currentEdgeType.getProperties().iterator();
						while(it.hasNext()) {
							currentProperty = it.next();
							type = handler.resolveType(currentProperty.getPropertyType().toLowerCase(Locale.ENGLISH), context);

							if(type != null) {
								newEdgeType.createProperty(currentProperty.getName(), type);
							}
							else {  
								it.remove();
								statistics.warningMessages.add(currentProperty.getPropertyType() + " type is not supported, the correspondent property will be dropped.");
							}
						}
						context.getOutputManager().debug("Edge-type '%s' wrote.\n", currentEdgeType.getName());
					}
					else {
						boolean updated = this.checkAndUpdateClass(orientGraph, currentEdgeType, handler, context);

						if(updated) {
							context.getOutputManager().debug("Edge-type '%s' updated.\n", currentEdgeType.getName());
						}
						else {
							context.getOutputManager().debug("Edge-type '%s' already present in the Orient schema.\n", currentEdgeType.getName());
						}
					}
					iteration++;
					statistics.wroteEdgeType++;
				}

				/*
				 *  Writing indexes on properties belonging to the original primary key
				 */

				context.getOutputManager().debug("Building indexes on properties belonging to the original primary key...");
				statistics.totalNumberOfIndices = numberOfVertices;

				String currentType = null;
				List<String> properties = null;
				iteration = 1;
				boolean isPresent;
				for(OVertexType currentVertexType: graphModel.getVerticesType()) {

					currentType = currentVertexType.getName();
					properties = new ArrayList<String>();
					for(OModelProperty currentProperty: currentVertexType.getProperties()) {
						if(currentProperty.isFromPrimaryKey()) {
							properties.add(currentProperty.getName());
						}
					}

					// check if vertex type is already present in the orient schema
					isPresent = orientGraph.getRawGraph().getMetadata().getIndexManager().existsIndex(currentType + ".pkey");

					if(!isPresent) {

						String propertiesList = "";
						int j = 0;
						for(String property: properties) {
							if(j == properties.size()-1) 
								propertiesList += property;
							else
								propertiesList += property + ",";
							j++;
						}

						if(!propertiesList.isEmpty()) {
							context.getOutputManager().debug("Building index for '%s' on %s  (%s/%s)...", currentVertexType.getName(), propertiesList, iteration, numberOfVertices);
							statement = "create index " + currentType + ".pkey" + " on " + currentType + " (" + propertiesList + ") unique_hash_index";
							sqlCommand = new OCommandSQL(statement);
							orientGraph.getRawGraph().command(sqlCommand).execute();
							context.getOutputManager().debug("Index for %s built.\n", currentVertexType.getName());
						}
						else {
							context.getStatistics().warningMessages.add("The table '" + currentVertexType.getName() + "' has not primary key constraints defined in the db schema,"
									+ " thus the correspondent Class Vertex in Orient will not have a default index on the original primary key.");
						}
					}
					else {
						context.getOutputManager().debug("Index for %s already present in the Orient schema.\n", currentVertexType.getName());
					}
					iteration++;
					statistics.wroteIndices++;
				}
			} catch(OException e) {
				if(e.getMessage() != null)
					context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
				else
					context.getOutputManager().error(e.getClass().getName());

				Writer writer = new StringWriter();
				e.printStackTrace(new PrintWriter(writer));
				context.getOutputManager().debug(writer.toString());
				System.exit(0);
			}    
			statistics.notifyListeners();
			statistics.runningStepNumber = -1;
			orientGraph.shutdown();
			
			success = true;
			return success;

		}
		else {
			context.getOutputManager().error("Changes on entities involved in hierarchical trees detected: Teleporter cannot support these variation and"
					+ "grant coherence between the two databases. Rebuild the schema from scratch.");
			return success;
		}
	}


	/**
	 * 
	 * 
	 * @param orientGraph
	 * @param currentElementType
	 * @return
	 */
	private boolean checkAndUpdateClass(OrientGraphNoTx orientGraph, OElementType currentElementType, ODriverDataTypeHandler handler, OTeleporterContext context) {

		boolean updated = false;

		OrientElementType orientElementType = null;

		if(currentElementType instanceof OVertexType) {
			orientElementType = orientGraph.getVertexType(currentElementType.getName());
		}
		else if(currentElementType instanceof OEdgeType) {
			orientElementType = orientGraph.getEdgeType(currentElementType.getName());
		}
		else {
			context.getOutputManager().error("Fatal error: current element type '%s' is not instance neither of Vertex Type neither of EdgeType", currentElementType.getName());
			System.exit(0);
		}

		OProperty orientSchemaProperty;
		OType type;

		// check from model properties
		Iterator<OModelProperty> it1 = currentElementType.getProperties().iterator();
		OModelProperty currentProperty;
		while(it1.hasNext()) {
			currentProperty = it1.next();
			orientSchemaProperty = orientElementType.getProperty(currentProperty.getName());
			type = handler.resolveType(currentProperty.getPropertyType().toLowerCase(Locale.ENGLISH), context);

			if(orientSchemaProperty != null) {
				// property present in orientdb schema, check if is it equal (type check), in case it's modified

				// if types are not equal the property will be dropped and added again with the correct type
				if(!currentProperty.getPropertyType().equalsIgnoreCase(type.toString())) {
					orientElementType.dropProperty(currentProperty.getName());
					orientElementType.createProperty(currentProperty.getName(), type);
					updated = true;
				}
			}
			else {
				// property not present in orientdb schema, then it's added (if type allows it)
				orientElementType.createProperty(currentProperty.getName(), type);
				updated = true;
			}
		}

		// check from orientdb schema properties
		OProperty orientSchemaProperty2;
		Iterator<OProperty> it2 = orientElementType.declaredProperties().iterator();
		List<String> toDrop = new LinkedList<String>();
		while(it2.hasNext()) {
			orientSchemaProperty2 = it2.next();
			// if the property is not present in the model vertex type, then is added to a "to-drop list"
			if(currentElementType.getPropertyByName(orientSchemaProperty2.getName()) == null) {
				toDrop.add(orientSchemaProperty2.getName());
				updated = true;
			}
		}

		// dropping properties
		for(String propertyName: toDrop) {
			orientElementType.dropProperty(propertyName);
		}

		return updated;
	}

	public boolean inheritanceChangesPresent(OGraphModel graphModel, OrientGraphNoTx orientGraph) {

		OrientVertexType orientCorrespondentVertexType;

		for(OVertexType currentVertexType: graphModel.getVerticesType()) {

			orientCorrespondentVertexType = orientGraph.getVertexType(currentVertexType.getName());

			// check for changes if vertex type is already present in the orient schema
			if(currentVertexType != null && orientCorrespondentVertexType != null) {

				if( (currentVertexType.getParentType() == null && !orientCorrespondentVertexType.getSuperClass().getName().equals("V"))
						|| (currentVertexType.getParentType() != null && orientCorrespondentVertexType.getSuperClass().getName().equals("V")) )
					return true;

				else if(currentVertexType.getParentType() != null && !orientCorrespondentVertexType.getSuperClass().getName().equals("V")) {
					if(!currentVertexType.getParentType().getName().equals(orientCorrespondentVertexType.getSuperClass().getName()))
						return true;
				}
			}

		}

		return false;

	}

}
