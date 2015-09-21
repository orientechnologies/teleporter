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

package com.orientechnologies.teleporter.factory;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.strategy.OImportStrategy;
import com.orientechnologies.teleporter.strategy.ONaiveAggregationImportStrategy;
import com.orientechnologies.teleporter.strategy.ONaiveImportStrategy;

/**
 * Factory used to instantiate the chosen strategy for the importing phase starting from its name.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class OStrategyFactory {

	public OStrategyFactory() {}

	public OImportStrategy buildStrategy(String chosenStrategy, OTeleporterContext context) {
		OImportStrategy strategy = null;

		if(chosenStrategy == null)  {
			strategy = new ONaiveAggregationImportStrategy();
		}
		else {
			switch(chosenStrategy) {

			case "naive":   strategy = new ONaiveImportStrategy();
			break;

			case "naive-aggregate":   strategy = new ONaiveAggregationImportStrategy();
			break;

			default :  context.getOutputManager().error("The typed strategy doesn't exist.\n");
			}
		}
		return strategy;
	}

}
