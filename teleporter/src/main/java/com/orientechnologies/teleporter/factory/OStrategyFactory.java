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

package com.orientechnologies.teleporter.factory;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.exception.OTeleporterIOException;
import com.orientechnologies.teleporter.strategy.OImportStrategy;
import com.orientechnologies.teleporter.strategy.mongodb.OMongoDBAggregateStrategy;
import com.orientechnologies.teleporter.strategy.mongodb.OMongoDBExpandStrategy;
import com.orientechnologies.teleporter.strategy.neo4j.ONeo4jNaiveStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveAggregationStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;

/**
 * Factory used to instantiate the chosen strategy for the importing phase starting from its name.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class OStrategyFactory {

	public OStrategyFactory() {}

	public OImportStrategy buildStrategy(String storageDriver, String chosenStrategy, OTeleporterContext context) throws OTeleporterIOException {
		
		OImportStrategy strategy = null;

		// choosing strategy for migration from neo4j
		if(storageDriver.equalsIgnoreCase("neo4j")) {

			if(chosenStrategy == null)  {
				strategy = new ONeo4jNaiveStrategy();
			}
			else {
				switch(chosenStrategy) {

				case "naive":   strategy = new ONeo4jNaiveStrategy();
				break;

				default :  context.getOutputManager().error("The typed strategy doesn't exist for migration from Neo4j.\n");
				}
			}
		}

		// choosing strategy for migration from mongoDB
		else if (storageDriver.equalsIgnoreCase("mongoDB")) {

			if(chosenStrategy == null)  {
				strategy = new OMongoDBExpandStrategy();
			}
			else {
				switch(chosenStrategy) {

				case "aggregate":   strategy = new OMongoDBAggregateStrategy();
				break;
				
				case "expand":   strategy = new OMongoDBExpandStrategy();
				break;

				default :  context.getOutputManager().error("The typed strategy doesn't exist for migration from MongoDB.\n");
				}
			}
		}

		// choosing strategy for migration from RDBSs
		else {

			if(chosenStrategy == null)  {
				strategy = new ODBMSNaiveAggregationStrategy();
			}
			else {
				switch(chosenStrategy) {

				case "naive":   strategy = new ODBMSNaiveStrategy();
				break;

				case "naive-aggregate":   strategy = new ODBMSNaiveAggregationStrategy();
				break;

				default :  context.getOutputManager().error("The typed strategy doesn't exist for migration from the chosen RDBMS.\n");
				}
			}
		}

		if(strategy == null)
			throw new OTeleporterIOException("Strategy not available for the chosen source.");

		return strategy;
	}

}
