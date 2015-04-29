/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.drakkar.factory;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.drakkar.strategy.OImportStrategy;
import com.orientechnologies.orient.drakkar.strategy.ONaiveAggregationImportStrategy;
import com.orientechnologies.orient.drakkar.strategy.ONaiveImportStrategy;

/**
 * Factory used to instantiate the chosen strategy for the importing phase starting from its name.
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 * 
 */

public class OStrategyFactory {
  
  public OStrategyFactory() {}

  public OImportStrategy buildStrategy(String chosenStrategy) {
    OImportStrategy strategy = null;

    switch(chosenStrategy) {

    case "naive":   strategy = new ONaiveImportStrategy();
                    break;

    case "naive-aggregate":   strategy = new ONaiveAggregationImportStrategy();
                              break;

    default :  OLogManager.instance().error(this, "Strategy doesn't exist.\n", (Object[])null);
    }

    return strategy;
  }

}
