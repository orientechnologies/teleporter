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

package com.orientechnologies.teleporter.util;

import java.util.Comparator;

import com.orientechnologies.teleporter.model.dbschema.ORelationship;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class RelationshipComparator implements Comparator<ORelationship> {

	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(ORelationship r1, ORelationship r2) {
		if(!r1.getForeignEntityName().equals(r2.getForeignEntityName()))
			return r1.getForeignEntityName().compareTo(r2.getForeignEntityName());
		else
			return r1.getForeignKey().getInvolvedAttributes().get(0).getName().compareTo(r2.getForeignKey().getInvolvedAttributes().get(0).getName());
	}

}
