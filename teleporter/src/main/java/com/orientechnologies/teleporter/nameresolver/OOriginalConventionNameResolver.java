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

package com.orientechnologies.teleporter.nameresolver;

import com.orientechnologies.teleporter.model.dbschema.ORelationship;

/**
 * Implementation of ONameResolver that maintains the original name convention.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OOriginalConventionNameResolver implements ONameResolver {


	@Override
	public String resolveVertexName(String candidateName) {

		if(candidateName.contains(" ")) {
			int pos;
			while(candidateName.contains(" ")) {
				pos = candidateName.indexOf(" ");
				candidateName = candidateName.substring(0,pos) + (candidateName.charAt(pos+1)+"").toUpperCase() + candidateName.substring(pos+2);
			}
		}

		return candidateName;
	}


	@Override
	public String resolveVertexProperty(String candidateName) {

		if(candidateName.contains(" ")) {
			int pos;
			while(candidateName.contains(" ")) {
				pos = candidateName.indexOf(" ");
				candidateName = candidateName.substring(0,pos) + (candidateName.charAt(pos+1)+"").toUpperCase() + candidateName.substring(pos+2);
			}
		}

		return candidateName;
	}


	@Override
	public String resolveEdgeName(ORelationship relationship) {
		
		String finalName;

		// Foreign Key composed of 1 attribute
		if(relationship.getForeignKey().getInvolvedAttributes().size() == 1) {
			String columnName = relationship.getForeignKey().getInvolvedAttributes().get(0).getName();
			columnName = columnName.replace("_id", "");
			columnName = columnName.replace("_ID", "");
			columnName = columnName.replace("_oid", "");
			columnName = columnName.replace("_OID", "");
			columnName = columnName.replace("_eid", "");
			columnName = columnName.replace("_EID", "");
			
			// manipulating name (Java Convention)
			finalName = "has_" + columnName;
		}

		// Foreign Key composed of multiple attribute
		else {         
			finalName = relationship.getForeignEntityName() + "2" + relationship.getParentEntityName();
		}

		return finalName;
	}


	@Override
	public String reverseTransformation(String transformedName) {
		return transformedName;
	}

}
