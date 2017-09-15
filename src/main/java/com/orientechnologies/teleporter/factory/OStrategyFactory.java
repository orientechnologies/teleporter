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
 * @email <g.ponzi--at--orientdb.com>
 */

public class OStrategyFactory {

  public OStrategyFactory() {
  }

  public OWorkflowStrategy buildStrategy(String storageDriver, String chosenStrategy) throws OTeleporterIOException {

    OWorkflowStrategy strategy = null;

    // choosing strategy for migration from RDBSs

    if (chosenStrategy == null) {
      strategy = new ODBMSNaiveAggregationStrategy();
    } else {
      switch (chosenStrategy) {

      case "naive":
        strategy = new ODBMSNaiveStrategy();
        break;

      case "naive-aggregate":
        strategy = new ODBMSNaiveAggregationStrategy();
        break;

      case "interactive":
        strategy = new ODBMSSimpleModelBuildingStrategy();
        break;

      case "interactive-aggr":
        strategy = new ODBMSModelBuildingAggregationStrategy();
        break;

      default:
        OTeleporterContext.getInstance().getMessageHandler()
            .error(this, "The typed strategy doesn't exist for migration from the chosen RDBMS.\n");
      }

      OTeleporterContext.getInstance().setExecutionStrategy(chosenStrategy);
    }

    if (strategy == null)
      throw new OTeleporterIOException("Strategy not available for the chosen source.");

    return strategy;
  }

}
