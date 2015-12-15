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

package com.orientechnologies.teleporter.model.graphmodel;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * It represents an Orient class. It could be a Vertex-Type or an Edge-Type in
 * the graph model.
 * 
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 *
 */

public class OElementType implements Comparable<OElementType> {

	protected String name;
	protected List<OModelProperty> properties;
	protected List<OModelProperty> inheritedProperties;
	protected Set<OModelProperty> allProperties;
	protected OElementType parentType;
	protected int inheritanceLevel;

	public OElementType(String type) {
		this.name = type;
		this.properties = new LinkedList<OModelProperty>();
		this.inheritedProperties = new LinkedList<OModelProperty>();
		this.allProperties = null;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String type) {
		this.name = type;
	}

	public List<OModelProperty> getProperties() {
		return this.properties;
	}

	public void setProperties(List<OModelProperty> properties) {
		this.properties = properties;
	}

	public List<OModelProperty> getInheritedProperties() {
		return this.inheritedProperties;
	}

	public void setInheritedProperties(List<OModelProperty> inheritedProperties) {
		this.inheritedProperties = inheritedProperties;
	}

	public OElementType getParentType() {
		return this.parentType;
	}

	public void setParentType(OElementType parentType) {
		this.parentType = parentType;
	}

	public int getInheritanceLevel() {
		return this.inheritanceLevel;
	}

	public void setInheritanceLevel(int inheritanceLevel) {
		this.inheritanceLevel = inheritanceLevel;
	}

	public void removePropertyByName(String toRemove) {
		Iterator<OModelProperty> it = this.properties.iterator();
		OModelProperty currentProperty = null;

		while (it.hasNext()) {
			currentProperty = it.next();
			if (currentProperty.getName().equals(toRemove))
				it.remove();
		}
	}

	public OModelProperty getPropertyByName(String name) {
		for (OModelProperty property : this.properties) {
			if (property.getName().equals(name)) {
				return property;
			}
		}
		return null;
	}

	public OModelProperty getInheritedPropertyByName(String name) {
		for (OModelProperty property : this.inheritedProperties) {
			if (property.getName().equals(name)) {
				return property;
			}
		}
		return null;
	}


	// Returns properties and inherited properties
	public Set<OModelProperty> getAllProperties() {
		
		if (allProperties == null) {
			allProperties = new LinkedHashSet<OModelProperty>();
			allProperties.addAll(this.inheritedProperties);
			allProperties.addAll(this.properties);
		}
		
		return allProperties;
	}

	@Override
	public int compareTo(OElementType toCompare) {

		if (this.inheritanceLevel > toCompare.getInheritanceLevel())
			return 1;
		else if (this.inheritanceLevel < toCompare.getInheritanceLevel())
			return -1;
		else
			return this.name.compareTo(toCompare.getName());

	}
}
