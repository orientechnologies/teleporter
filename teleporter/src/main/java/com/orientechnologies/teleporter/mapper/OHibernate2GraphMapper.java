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

package com.orientechnologies.teleporter.mapper;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.model.dbschema.OAttribute;
import com.orientechnologies.teleporter.model.dbschema.OEntity;
import com.orientechnologies.teleporter.model.dbschema.OHierarchicalBag;

/**
 * Extends OER2GraphMapper thus manages the source DB schema and the destination graph model with their correspondences.
 * Unlike the superclass, this class builds the source DB schema starting from Hibernate's XML configuration file.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OHibernate2GraphMapper extends OER2GraphMapper {

	private String xmlPath;

	public OHibernate2GraphMapper(String driver, String uri, String username, String password, String xmlPath, List<String> includedTables, List<String> excludedTables) {
		super(driver, uri, username, password, includedTables, excludedTables);
		this.xmlPath = xmlPath;
	}

	@Override
	public void buildSourceSchema(OTeleporterContext context) {

		try {

			/*
			 * Building Info from DB Schema
			 */

			super.buildSourceSchema(context);


			/*
			 * XML Checking and Inheritance
			 */

			// XML parsing and DOM building

			File xmlFile = new File(this.xmlPath);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document dom = dBuilder.parse(xmlFile);

			NodeList entities = dom.getElementsByTagName("class");
			Element currentEntityElement;
			OEntity currentEntity = null;

			for(int i=0; i<entities.getLength(); i++) {
				currentEntityElement = (Element) entities.item(i);

				if(currentEntityElement.hasAttribute("table"))
					currentEntity = super.dataBaseSchema.getEntityByNameIgnoreCase(currentEntityElement.getAttribute("table"));
				else {
					context.getOutputManager().error("XML Format error: problem in class definition, table attribute missing in the class node.");
					System.exit(0);
				}

				// inheritance
				if(currentEntity != null)
					this.detectInheritanceAndUpdateSchema(currentEntity, currentEntityElement, context);
			}

			// sorting tables for inheritance level and then for name
			Collections.sort(super.dataBaseSchema.getEntities());

		}catch(Exception e) {
			if(e.getMessage() != null)
				context.getOutputManager().error(e.getClass().getName() + " - " + e.getMessage());
			else
				context.getOutputManager().error(e.getClass().getName());
			System.out.println(e.getMessage());
			e.printStackTrace();
			Writer writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));
			String s = writer.toString();
			context.getOutputManager().debug("\n" + s + "\n");
			System.exit(0);
		}

	}


	private void detectInheritanceAndUpdateSchema(OEntity parentEntity, Element parentEntityElement, OTeleporterContext context) {

		NodeList subclassElements = parentEntityElement.getElementsByTagName("subclass");
		NodeList joinedSubclassElements = parentEntityElement.getElementsByTagName("joined-subclass");
		NodeList unionSubclassElements = parentEntityElement.getElementsByTagName("union-subclass");
		Element discriminatorElement = (Element) parentEntityElement.getElementsByTagName("discriminator").item(0);

		OHierarchicalBag hierarchicalBag = new OHierarchicalBag();
		String rootDiscriminatorValue = null;

		// TABLE PER CLASS Hierarchy or Table per Subclass Inheritance
		if(subclassElements.getLength() > 0) {
			if(parentEntityElement.hasAttribute("discriminator-value")) 
				rootDiscriminatorValue = parentEntityElement.getAttribute("discriminator-value");
			this.performSubclassTagInheritance(hierarchicalBag, parentEntity, subclassElements, discriminatorElement, rootDiscriminatorValue, context);
		}

		// TABLE PER SUBCLASS Inheritance
		if(joinedSubclassElements.getLength() > 0) {

			// initializing the hierarchical bag
			hierarchicalBag.setInheritancePattern("table-per-type");
			super.dataBaseSchema.getHierarchicalBags().add(hierarchicalBag);
			if(hierarchicalBag.getDepth2entities().get(parentEntity.getInheritanceLevel()) == null) {
				Set<OEntity> tmp = new LinkedHashSet<OEntity>();
				tmp.add(parentEntity);
				hierarchicalBag.getDepth2entities().put(parentEntity.getInheritanceLevel(), tmp);
				parentEntity.setHierarchicalBag(hierarchicalBag);
			}
			if(discriminatorElement != null) {
				hierarchicalBag.setDiscriminatorColumn(discriminatorElement.getAttribute("column"));
			}

			this.performJoinedSubclassTagInheritance(hierarchicalBag, parentEntity, joinedSubclassElements, context);
		}

		// TABLE PER CONCRETE CLASS Inheritance
		if(unionSubclassElements.getLength() > 0) {

			// initializing the hierarchical bag
			hierarchicalBag.setInheritancePattern("table-per-concrete-type");
			super.dataBaseSchema.getHierarchicalBags().add(hierarchicalBag);
			if(hierarchicalBag.getDepth2entities().get(parentEntity.getInheritanceLevel()) == null) {
				Set<OEntity> tmp = new LinkedHashSet<OEntity>();
				tmp.add(parentEntity);
				hierarchicalBag.getDepth2entities().put(parentEntity.getInheritanceLevel(), tmp);
				parentEntity.setHierarchicalBag(hierarchicalBag);
			}
			if(discriminatorElement != null) {
				hierarchicalBag.setDiscriminatorColumn(discriminatorElement.getAttribute("column"));
			}

			this.performUnionSubclassTagInheritance(hierarchicalBag, parentEntity, unionSubclassElements, context);
		}

	}


	// Table per Class Hierarchy or Table per Subclass Inheritance
	private void performSubclassTagInheritance(OHierarchicalBag hierarchicalBag, OEntity parentEntity, NodeList subclassElements, Element discriminatorElement, String rootDiscriminatorValue, OTeleporterContext context) {

		NodeList joinElements;
		Element currentEntityElement;
		String currentEntityElementName = null;
		OEntity currentChildEntity;

		// distinguishing between "Table Per Class Hierarchy" and "Table Per Subclass" inheritance
		currentEntityElement = (Element)subclassElements.item(0);
		joinElements = currentEntityElement.getElementsByTagName("join");

		// Table Per Subclass inheritance when join elements are present
		if(joinElements.getLength()>0) {

			// initializing the hierarchical bag
			hierarchicalBag.setInheritancePattern("table-per-type");
			super.dataBaseSchema.getHierarchicalBags().add(hierarchicalBag);
			if(hierarchicalBag.getDepth2entities().get(parentEntity.getInheritanceLevel()) == null) {
				Set<OEntity> tmp = new LinkedHashSet<OEntity>();
				tmp.add(parentEntity);
				hierarchicalBag.getDepth2entities().put(parentEntity.getInheritanceLevel(), tmp);
				parentEntity.setHierarchicalBag(hierarchicalBag);
			}
			if(discriminatorElement != null) {
				hierarchicalBag.setDiscriminatorColumn(discriminatorElement.getAttribute("column"));
			}

			for(int j=0; j<subclassElements.getLength(); j++) {
				currentEntityElement = (Element)subclassElements.item(j);
				joinElements = currentEntityElement.getElementsByTagName("join");
				performJoinedSubclassTagInheritance(hierarchicalBag, parentEntity, joinElements, context);
			}
		}

		// Table per Class Hierarchy
		else {

			// initializing the hierarchical bag
			hierarchicalBag.setInheritancePattern("table-per-hierarchy");
			super.dataBaseSchema.getHierarchicalBags().add(hierarchicalBag);
			if(hierarchicalBag.getDepth2entities().get(parentEntity.getInheritanceLevel()) == null) {
				Set<OEntity> tmp = new LinkedHashSet<OEntity>();
				tmp.add(parentEntity);
				hierarchicalBag.getDepth2entities().put(parentEntity.getInheritanceLevel(), tmp);
				parentEntity.setHierarchicalBag(hierarchicalBag);
			}
			if(discriminatorElement != null) {
				hierarchicalBag.setDiscriminatorColumn(discriminatorElement.getAttribute("column"));
			}
			hierarchicalBag.getEntityName2discriminatorValue().put(parentEntity.getName(), rootDiscriminatorValue);

			for(int i=0; i<subclassElements.getLength(); i++) {

				currentEntityElement = (Element)subclassElements.item(i);

				if(currentEntityElement.hasAttribute("name"))
					currentEntityElementName = currentEntityElement.getAttribute("name");
				else {
					context.getOutputManager().error("XML Format error: problem in subclass definition, table attribute missing in the joined-subclass nodes.");
					System.exit(0);
				}
				currentChildEntity = new OEntity(currentEntityElementName, null);

				// entity's attributes setting
				String discriminatorColumnName = discriminatorElement.getAttribute("column");
				parentEntity.removeAttributeByNameIgnoreCase(discriminatorColumnName);
				parentEntity.renumberAttributesOrdinalPositions();

				// primary key setting
				currentChildEntity.setPrimaryKey(parentEntity.getPrimaryKey());

				NodeList propertiesElements = currentEntityElement.getElementsByTagName("property");
				Element currentPropertyElement;
				OAttribute currentChildAttribute;
				OAttribute currentParentCorrespondingAttribute;

				for(int j=0; j<propertiesElements.getLength(); j++) {
					currentPropertyElement = (Element) propertiesElements.item(j);
					currentParentCorrespondingAttribute = parentEntity.getAttributeByNameIgnoreCase(currentPropertyElement.getAttribute("column"));

					// building child's attribute and removing the corresponding attribute from the parent entity
					currentChildAttribute = new OAttribute(currentParentCorrespondingAttribute.getName(), j+1, currentParentCorrespondingAttribute.getDataType(), currentChildEntity);
					currentChildEntity.addAttribute(currentChildAttribute);
					parentEntity.getAttributes().remove(currentParentCorrespondingAttribute);
				}

				parentEntity.renumberAttributesOrdinalPositions();

				super.dataBaseSchema.getEntities().add(currentChildEntity);
				currentChildEntity.setParentEntity(parentEntity);
				currentChildEntity.setInheritanceLevel(parentEntity.getInheritanceLevel()+1);

				// updating hierarchical bag
				if(hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel()) == null) {
					Set<OEntity> tmp = new LinkedHashSet<OEntity>();
					tmp.add(currentChildEntity);
					hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
				}
				else {
					Set<OEntity> tmp = hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel());
					tmp.add(currentChildEntity);
					hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
				}
				currentChildEntity.setHierarchicalBag(hierarchicalBag);
				hierarchicalBag.getEntityName2discriminatorValue().put(currentChildEntity.getName(), currentEntityElement.getAttribute("discriminator-value"));

			}
		}
	}

	// Table per Subclass Inheritance
	private void performJoinedSubclassTagInheritance(OHierarchicalBag hierarchicalBag, OEntity parentEntity, NodeList joinedSubclassElements, OTeleporterContext context) {

		Element currentChildElement;
		OEntity currentChildEntity;
		String currentChildEntityName = null;

		for(int i=0; i<joinedSubclassElements.getLength(); i++) {
			currentChildElement = (Element) joinedSubclassElements.item(i);
			if(currentChildElement.hasAttribute("table"))
				currentChildEntityName = currentChildElement.getAttribute("table");
			else {
				context.getOutputManager().error("XML Format error: problem in subclass definition, table attribute missing in the joined-subclass nodes.");
				System.exit(0);
			}
			currentChildEntity = super.dataBaseSchema.getEntityByNameIgnoreCase(currentChildEntityName);
			currentChildEntity.setParentEntity(parentEntity);
			currentChildEntity.setInheritanceLevel(parentEntity.getInheritanceLevel()+1);

			// removing attributes belonging to the primary key
			OAttribute currentAttribute;
			Iterator<OAttribute> it = currentChildEntity.getAttributes().iterator();
			while(it.hasNext()) {
				currentAttribute = it.next();
				if(currentChildEntity.getPrimaryKey().getInvolvedAttributes().contains(currentAttribute)) {
					it.remove();
				}
			}
			currentChildEntity.renumberAttributesOrdinalPositions();

			// updating hierarchical bag
			if(hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel()) == null) {
				Set<OEntity> tmp = new LinkedHashSet<OEntity>();
				tmp.add(currentChildEntity);
				hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
			}
			else {
				Set<OEntity> tmp = hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel());
				tmp.add(currentChildEntity);
				hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
			}
			currentChildEntity.setHierarchicalBag(hierarchicalBag);

			// recursive call on the node
			this.detectInheritanceAndUpdateSchema(currentChildEntity, currentChildElement, context);
		}

	}

	// Table per Concrete Class
	void performUnionSubclassTagInheritance(OHierarchicalBag hierarchicalBag, OEntity parentEntity, NodeList unionSubclassElements, OTeleporterContext context) {

		Element currentChildElement;
		OEntity currentChildEntity;
		String currentChildEntityName = null;

		for(int i=0; i<unionSubclassElements.getLength(); i++) {
			currentChildElement = (Element) unionSubclassElements.item(i);

			if(currentChildElement.hasAttribute("table"))
				currentChildEntityName = currentChildElement.getAttribute("table");
			else {
				context.getOutputManager().error("XML Format error: problem in subclass definition, table attribute missing in the joined-subclass nodes.");
				System.exit(0);
			}

			currentChildEntity = super.dataBaseSchema.getEntityByNameIgnoreCase(currentChildEntityName);
			currentChildEntity.setParentEntity(parentEntity);
			currentChildEntity.setInheritanceLevel(parentEntity.getInheritanceLevel()+1);

			// removing attributes belonging to the primary key
			OAttribute currentAttribute;
			Iterator<OAttribute> it = currentChildEntity.getAttributes().iterator();
			while(it.hasNext()) {
				currentAttribute = it.next();
				if(currentChildEntity.getPrimaryKey().getInvolvedAttributes().contains(currentAttribute)) {
					it.remove();
				}
			}
			currentChildEntity.renumberAttributesOrdinalPositions();

			// updating hierarchical bag
			if(hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel()) == null) {
				Set<OEntity> tmp = new LinkedHashSet<OEntity>();
				tmp.add(currentChildEntity);
				hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
			}
			else {
				Set<OEntity> tmp = hierarchicalBag.getDepth2entities().get(currentChildEntity.getInheritanceLevel());
				tmp.add(currentChildEntity);
				hierarchicalBag.getDepth2entities().put(currentChildEntity.getInheritanceLevel(), tmp);
			}
			currentChildEntity.setHierarchicalBag(hierarchicalBag);

			// recursive call on the node
			this.detectInheritanceAndUpdateSchema(currentChildEntity, currentChildElement, context);

			// removing inherited attributes
			it = currentChildEntity.getAttributes().iterator();
			while(it.hasNext()) {
				currentAttribute = it.next();
				if(parentEntity.getAttributes().contains(currentAttribute)) {
					it.remove();
					currentChildEntity.getInheritedAttributes().add(currentAttribute);
				}
			}
			currentChildEntity.renumberAttributesOrdinalPositions();
		}
	}

}
