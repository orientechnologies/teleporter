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

import java.util.List;

import com.orientechnologies.teleporter.context.OTeleporterContext;

/**
 * Interface that represents a specific approach of data importing.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public interface OImportStrategy {

	public void executeStrategy(String driver, String uri, String username, String password, String outOrientGraphUri, String chosenMapper, String xmlPath, String nameResolverConvention,
	    List<String> includedTables, List<String> excludedTables, OTeleporterContext context);
	
}
