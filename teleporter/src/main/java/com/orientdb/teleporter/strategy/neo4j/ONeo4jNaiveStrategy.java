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

package com.orientdb.teleporter.strategy.neo4j;

import java.util.List;

import com.orientdb.teleporter.context.OTeleporterContext;
import com.orientdb.teleporter.strategy.OImportStrategy;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ONeo4jNaiveStrategy implements OImportStrategy {

  @Override
  public void executeStrategy(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, String nameResolverConvention, List<String> includedTables,
      List<String> excludedTables, OTeleporterContext context) {
    // TODO

  }

}
