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
import com.orientechnologies.teleporter.strategy.OWorkflowStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSModelBuildingAggregationStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveAggregationStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSSimpleModelBuildingStrategy;

/**
 * Factory used to instantiate the chosen strategy for the importing phase starting from its name.
 *
 * @author Gabriele Ponzi
 * @email  <g.ponzi--at--orientdb.com>
 *
 */

public class OStrategyFactory {

  public OStrategyFactory() {}

  public OWorkflowStrategy buildStrategy(String storageDriver, String chosenStrategy) throws
          OTeleporterIOException {

    OWorkflowStrategy strategy = null;

    // choosing strategy for migration from RDBSs

    if(chosenStrategy == null)  {
      strategy = new ODBMSNaiveAggregationStrategy();
    }
    else {
      switch(chosenStrategy) {

        case "naive":   strategy = new ODBMSNaiveStrategy();
          break;

        case "naive-aggregate":   strategy = new ODBMSNaiveAggregationStrategy();
          break;

        case "interactive":   strategy = new ODBMSSimpleModelBuildingStrategy();
          break;

        case "interactive-aggr":   strategy = new ODBMSModelBuildingAggregationStrategy();
          break;

        default :  OTeleporterContext.getInstance().getOutputManager().error("The typed strategy doesn't exist for migration from the chosen RDBMS.\n");
      }

      OTeleporterContext.getInstance().setExecutionStrategy(chosenStrategy);
    }

    if(strategy == null)
      throw new OTeleporterIOException("Strategy not available for the chosen source.");

    return strategy;
  }

}
