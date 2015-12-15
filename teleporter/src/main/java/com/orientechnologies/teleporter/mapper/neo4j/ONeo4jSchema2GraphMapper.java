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

package com.orientechnologies.teleporter.mapper.neo4j;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.mapper.OSource2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.ODataSourceSchema;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ONeo4jSchema2GraphMapper extends OSource2GraphMapper {

	@Override
	public ODataSourceSchema getSourceSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void buildSourceSchema(OTeleporterContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void buildGraphModel(ONameResolver nameResolver, OTeleporterContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}

}
