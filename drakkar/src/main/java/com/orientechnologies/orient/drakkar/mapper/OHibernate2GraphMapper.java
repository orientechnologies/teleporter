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

package com.orientechnologies.orient.drakkar.mapper;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.model.dbschema.OAttribute;
import com.orientechnologies.orient.drakkar.model.dbschema.OEntity;
import com.orientechnologies.orient.drakkar.model.dbschema.OHierarchicalBag;

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

  public OHibernate2GraphMapper(String driver, String uri, String username, String password, String xmlPath) {
    super(driver, uri, username, password);
    this.xmlPath = xmlPath;
  }

  @Override
  public void buildSourceSchema(ODrakkarContext context) {

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
          context.getOutputManager().error("XML Format ERROR: problem in class definition, table attribute missing on class node.");
          System.exit(0);
        }

        // inheritance
        this.detectInheritanceAndUpdateSchema(currentEntity, currentEntityElement, context);
      }

      // sorting tables for inheritance level and then for name
      Collections.sort(super.dataBaseSchema.getEntities());

    }catch(Exception e) {
      e.printStackTrace();
    }

  }


  private void detectInheritanceAndUpdateSchema(OEntity parentEntity, Element parentEntityElement, ODrakkarContext context) {

    NodeList subclassElements = parentEntityElement.getElementsByTagName("subclass");
    NodeList joinedSubclassElements = parentEntityElement.getElementsByTagName("joined-subclass");
    NodeList unionSubclassElements = parentEntityElement.getElementsByTagName("union-subclass");
    Element discriminatorElement = (Element) parentEntityElement.getElementsByTagName("discriminator").item(0);
    
    OHierarchicalBag hierarchicalBag = new OHierarchicalBag();

    // Table per Class Hierarchy or Table per Subclass Inheritance
    if(subclassElements.getLength() > 0) {
      this.performSubclassTagInheritance(hierarchicalBag, parentEntity, subclassElements, discriminatorElement, context);
    }

    // Table per Subclass Inheritance
    if(joinedSubclassElements.getLength() > 0) {
      this.performJoinedSubclassTagInheritance(hierarchicalBag, parentEntity, joinedSubclassElements, context);
    }

    // Table per Concrete Class Inheritance
    if(unionSubclassElements.getLength() > 0) {
      this.performUnionSubclassTagInheritance(hierarchicalBag, parentEntity, unionSubclassElements, context);
    }

  }


  // Table per Class Hierarchy or Table per Subclass Inheritance
  private void performSubclassTagInheritance(OHierarchicalBag hierarchicalBag, OEntity parentEntity, NodeList subclassElements, Element discriminatorElement, ODrakkarContext context) {

    NodeList joinElements;
    Element currentEntityElement;
    String currentEntityElementName = null;
    OEntity currentChildEntity;
    
//    hierarchicalBag.setInheritancePattern("table-per-class-hierarchy");
//    
//    if(hierarchicalBag.getDepth2entities().get(0) == null) {
//      Set<OEntity> tmp = new HashSet<OEntity>();
//      tmp.add(parentEntity);
//      hierarchicalBag.getDepth2entities().put(0, tmp);
//    }
//    
    

    for(int i=0; i<subclassElements.getLength(); i++) {

      currentEntityElement = (Element)subclassElements.item(i);
      joinElements = currentEntityElement.getElementsByTagName("join");

      // Table per subclass inheritance when join elements are present
      if(joinElements.getLength()>0)
        performJoinedSubclassTagInheritance(hierarchicalBag, parentEntity, joinElements, context);

      // Table per Class Hierarchy
      else {
        
        if(currentEntityElement.hasAttribute("name"))
          currentEntityElementName = currentEntityElement.getAttribute("name");
        else {
          context.getOutputManager().error("XML Format ERROR: problem in subclass definition, table attribute missing on joined-subclass nodes.");
          System.exit(0);
        }
        currentChildEntity = new OEntity(currentEntityElementName);

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
          currentParentCorrespondingAttribute = parentEntity.getAttributeByNameIgnoreCase(currentPropertyElement.getAttribute("name"));

          // building child's attribute and removing the corresponding attribute from the parent entity
          currentChildAttribute = new OAttribute(currentParentCorrespondingAttribute.getName(), j+1, currentParentCorrespondingAttribute.getDataType(), currentChildEntity);
          currentChildEntity.addAttribute(currentChildAttribute);
          parentEntity.removeAttribute(currentParentCorrespondingAttribute);
        }

        super.dataBaseSchema.getEntities().add(currentChildEntity);
        currentChildEntity.setParentEntity(parentEntity);
        currentChildEntity.setInheritanceLevel(parentEntity.getInheritanceLevel()+1);

      }
    }
  }

  // Table per Subclass Inheritance
  private void performJoinedSubclassTagInheritance(OHierarchicalBag hierarchicalBag, OEntity parentEntity, NodeList joinedSubclassElements, ODrakkarContext context) {

    Element currentChildElement;
    OEntity currentChildEntity;
    String currentChildEntityName = null;

    for(int i=0; i<joinedSubclassElements.getLength(); i++) {
      currentChildElement = (Element) joinedSubclassElements.item(i);
      if(currentChildElement.hasAttribute("table"))
        currentChildEntityName = currentChildElement.getAttribute("table");
      else {
        context.getOutputManager().error("XML Format ERROR: problem in subclass definition, table attribute missing on joined-subclass nodes.");
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

      // recursive call on the node
      this.detectInheritanceAndUpdateSchema(currentChildEntity, currentChildElement, context);
    }

  }

  // Table per Concrete Class
  void performUnionSubclassTagInheritance(OHierarchicalBag hierarchicalBag, OEntity parentEntity, NodeList unionSubclassElements, ODrakkarContext context) {

    Element currentChildElement;
    OEntity currentChildEntity;
    String currentChildEntityName = null;

    for(int i=0; i<unionSubclassElements.getLength(); i++) {
      currentChildElement = (Element) unionSubclassElements.item(i);

      if(currentChildElement.hasAttribute("table"))
        currentChildEntityName = currentChildElement.getAttribute("table");
      else {
        context.getOutputManager().error("XML Format ERROR: problem in subclass definition, table attribute missing on joined-subclass nodes.");
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

      // recursive call on the node
      this.detectInheritanceAndUpdateSchema(currentChildEntity, currentChildElement, context);

      // removing inherited attributes
      it = currentChildEntity.getAttributes().iterator();
      while(it.hasNext()) {
        currentAttribute = it.next();
        if(parentEntity.getAttributes().contains(currentAttribute))
          it.remove();
      }
      currentChildEntity.renumberAttributesOrdinalPositions();
    }
  }

}
