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

package com.orientechnologies.teleporter.writer;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.model.graphmodel.*;
import com.orientechnologies.teleporter.persistence.handler.ODriverDataTypeHandler;
import com.tinkerpop.blueprints.impls.orient.*;

import java.util.*;

/**
 * Writer that has the responsibility to write the model of the destination Orient Graph
 * on OrientDB as an OrientDB Schema.
 *
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OGraphModelWriter {

  private Map<String,OType> orientdbTypeName2orientdbType;

  public OGraphModelWriter() {
    this.init();
  }

  private void init() {
    this.orientdbTypeName2orientdbType = new HashMap<String,OType>();

    this.orientdbTypeName2orientdbType.put("boolean",OType.BOOLEAN);
    this.orientdbTypeName2orientdbType.put("integer",OType.INTEGER);
    this.orientdbTypeName2orientdbType.put("decimal",OType.DECIMAL);
    this.orientdbTypeName2orientdbType.put("short",OType.SHORT);
    this.orientdbTypeName2orientdbType.put("long",OType.LONG);
    this.orientdbTypeName2orientdbType.put("float",OType.FLOAT);
    this.orientdbTypeName2orientdbType.put("double",OType.DOUBLE);
    this.orientdbTypeName2orientdbType.put("datetime",OType.DATETIME);
    this.orientdbTypeName2orientdbType.put("date",OType.DATE);
    this.orientdbTypeName2orientdbType.put("string",OType.STRING);
    this.orientdbTypeName2orientdbType.put("binary",OType.BINARY);
    this.orientdbTypeName2orientdbType.put("byte",OType.BYTE);
  }


  public boolean writeModelOnOrient(OGraphModel graphModel, ODriverDataTypeHandler handler, String outOrientGraphUri) {
    boolean success = false;

    OrientBaseGraph orientGraph = null;
    OrientGraphFactory factory = new OrientGraphFactory(outOrientGraphUri,"admin","admin");
    try {
      orientGraph = factory.getNoTx();
    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();
    statistics.startWork3Time = new Date();
    statistics.runningStepNumber = 3;

    int numberOfVertices = graphModel.getVerticesType().size();
    statistics.totalNumberOfVertexTypes = numberOfVertices;
    int numberOfEdges = graphModel.getEdgesType().size();
    statistics.totalNumberOfEdgeTypes = numberOfEdges;
    statistics.totalNumberOfIndices = numberOfVertices;

    // deleting orient classes not present in the current graph model
    Collection<OClass> orientClasses = orientGraph.getRawGraph().getMetadata().getSchema().getClasses();
    for(OClass currOrientClass: orientClasses) {
      String orientClassName = currOrientClass.getName();
      if(! (orientClassName.startsWith("O") || orientClassName.startsWith("V") || orientClassName.startsWith("E") || orientClassName.startsWith("_")) ) {
        if (graphModel.getVertexTypeByNameIgnoreCase(orientClassName) == null && graphModel.getEdgeTypeByNameIgnoreCase(orientClassName) == null) {
          orientGraph.getRawGraph().getMetadata().getSchema().dropClass(orientClassName);
        }
      }
    }

    if(!this.inheritanceChangesPresent(graphModel, orientGraph)) {

      try {

        /*
         * Writing vertex-type
         */

        OTeleporterContext.getInstance().getOutputManager().debug("\nWriting vertex-types on OrientDB Schema...\n");

        OrientVertexType newVertexType;
        String statement;
        OCommandSQL sqlCommand;
        OType type;
        Iterator<OModelProperty> it = null;

        int iteration = 1;
        for(OVertexType currentVertexType: graphModel.getVerticesType()) {
          OTeleporterContext.getInstance().getOutputManager()
                  .debug("\nWriting '%s' vertex-type  (%s/%s)...\n", currentVertexType.getName(), iteration, numberOfVertices);

          // check if vertex type is already present in the orient schema
          newVertexType = orientGraph.getVertexType(currentVertexType.getName());

          if(newVertexType == null) {

            // inheritance case
            if(currentVertexType.getParentType() != null)
              newVertexType = orientGraph.createVertexType(currentVertexType.getName(), currentVertexType.getParentType().getName());
            else
              newVertexType = orientGraph.createVertexType(currentVertexType.getName());

            OModelProperty currentProperty = null;
            it = currentVertexType.getProperties().iterator();

            while(it.hasNext()) {
              currentProperty = it.next();
              if(currentProperty.isIncludedInMigration()) {
                if(currentProperty.getOrientdbType() == null) {
                  type = handler.resolveType(currentProperty.getOriginalType().toLowerCase(Locale.ENGLISH));
                }
                else {
                  type = this.resolveOrientDBType(currentProperty.getOrientdbType());
                }
                if (type != null) {
                  String propertyName = currentProperty.getName();
                  OProperty orientdbProperty = newVertexType.createProperty(propertyName, type);

                  // setting constraints if present
                  if (currentProperty.isMandatory() != null) {
                    orientdbProperty.setMandatory(currentProperty.isMandatory());
                  }
                  if (currentProperty.isReadOnly() != null) {
                    orientdbProperty.setReadonly(currentProperty.isReadOnly());
                  }
                  if (currentProperty.isNotNull() != null) {
                    orientdbProperty.setNotNull(currentProperty.isNotNull());
                  }
                } else {
                  it.remove();
                  statistics.warningMessages.add(currentProperty.getOriginalType() + " type is not supported, the correspondent property will be dropped.");
                }
              }
            }
            OTeleporterContext.getInstance().getOutputManager().debug("\nVertex-type '%s' wrote.\n", currentVertexType.getName());
          }
          else  {
            boolean updated = this.checkAndUpdateClass(orientGraph, currentVertexType, handler);

            if(updated) {
              OTeleporterContext.getInstance().getOutputManager().debug("\nVertex-type '%s' updated.\n", currentVertexType.getName());
            }
            else {
              OTeleporterContext.getInstance().getOutputManager().debug("\nVertex-type '%s' already present in the Orient schema.\n", currentVertexType.getName());
            }
          }

          iteration++;
          statistics.wroteVertexType++;
        }

        /*
         * Writing edge-type
         */

        OTeleporterContext.getInstance().getOutputManager().debug("\nWriting edge-types on OrientDB Schema...\n");

        OrientEdgeType newEdgeType;

        iteration = 1;
        for(OEdgeType currentEdgeType: graphModel.getEdgesType()) {
          OTeleporterContext.getInstance().getOutputManager().debug("\nWriting '%s' edge-type  (%s/%s)...\n", currentEdgeType.getName(), iteration, numberOfEdges);

          // check if edge type is already present in the orient schema
          newEdgeType = orientGraph.getEdgeType(currentEdgeType.getName());

          if(newEdgeType == null) {
            newEdgeType = orientGraph.createEdgeType(currentEdgeType.getName());
            OModelProperty currentProperty = null;
            it = currentEdgeType.getProperties().iterator();

            while(it.hasNext()) {
              currentProperty = it.next();
              if (currentProperty.isIncludedInMigration()) {
                if(currentProperty.getOrientdbType() == null) {
                  type = handler.resolveType(currentProperty.getOriginalType().toLowerCase(Locale.ENGLISH));
                }
                else {
                  type = this.resolveOrientDBType(currentProperty.getOrientdbType());
                }
                if (type != null) {
                  OProperty orientdbProperty = newEdgeType.createProperty(currentProperty.getName(), type);

                  // setting constraints if present
                  if (currentProperty.isMandatory() != null) {
                    orientdbProperty.setMandatory(currentProperty.isMandatory());
                  }
                  if (currentProperty.isReadOnly() != null) {
                    orientdbProperty.setReadonly(currentProperty.isReadOnly());
                  }
                  if (currentProperty.isNotNull() != null) {
                    orientdbProperty.setNotNull(currentProperty.isNotNull());
                  }
                } else {
                  it.remove();
                  statistics.warningMessages.add(currentProperty.getOriginalType() + " type is not supported, the correspondent property will be dropped.");
                }
              }
            }
            OTeleporterContext.getInstance().getOutputManager().debug("\nEdge-type '%s' wrote.\n", currentEdgeType.getName());
          }
          else {
            boolean updated = this.checkAndUpdateClass(orientGraph, currentEdgeType, handler);

            if(updated) {
              OTeleporterContext.getInstance().getOutputManager().debug("\nEdge-type '%s' updated.\n", currentEdgeType.getName());
            }
            else {
              OTeleporterContext.getInstance().getOutputManager().debug("\nEdge-type '%s' already present in the Orient schema.\n", currentEdgeType.getName());
            }
          }
          iteration++;
          statistics.wroteEdgeType++;
        }

        /*
         *  Writing indexes on properties belonging to the original primary key
         */

        OTeleporterContext.getInstance().getOutputManager().debug("\nBuilding indexes on properties belonging to the original primary keys...\n");

        String currentType = null;
        List<String> properties = null;
        iteration = 1;
        OIndexManagerProxy indexManager = orientGraph.getRawGraph().getMetadata().getIndexManager();
        boolean isPresent;
        for(OVertexType currentVertexType: graphModel.getVerticesType()) {

          currentType = currentVertexType.getName();
          properties = new ArrayList<String>();
          for(OModelProperty currentProperty: currentVertexType.getProperties()) {
            if(currentProperty.isFromPrimaryKey()) {
              properties.add(currentProperty.getName());
            }
          }

          // checking if the old index is based on the same properties of the current Class, if not it will be deleted
          String indexClassName = currentType + ".pkey";
          OIndex<?> classIndex = indexManager.getClassIndex(currentType, indexClassName);
          if(classIndex != null) {
            List<String> fieldNames = classIndex.getDefinition().getFields();

            for (String field : fieldNames) {
              if (!properties.contains(field)) {
                indexManager.dropIndex(indexClassName);
                break;
              }
            }
          }

          // check if vertex type is already present in the orient schema
          isPresent = indexManager.existsIndex(indexClassName);

          if(!isPresent) {

            String propertiesList = "";
            int j = 0;
            for(String property: properties) {
              if(j == properties.size()-1)
                propertiesList += property;
              else
                propertiesList += property + ",";
              j++;
            }

            if(!propertiesList.isEmpty()) {
              OTeleporterContext.getInstance().getOutputManager()
                      .debug("\nBuilding index for '%s' on %s  (%s/%s)...\n", currentVertexType.getName(), propertiesList, iteration, numberOfVertices);
              statement = "create index `" + currentType + ".pkey`" + " on `" + currentType + "` (" + propertiesList + ") unique_hash_index";
              sqlCommand = new OCommandSQL(statement);
              orientGraph.getRawGraph().command(sqlCommand).execute();
              OTeleporterContext.getInstance().getOutputManager().debug("\nIndex for %s built.\n", currentVertexType.getName());
            }
            else {
              OTeleporterContext.getInstance().getStatistics().warningMessages.add("The table '" + currentVertexType.getName() + "' has not primary key constraints defined in the db schema,"
                      + " thus the correspondent Class Vertex in Orient will not have a default index on the property deriving from the original primary key.");
            }
          }
          else {
            OTeleporterContext.getInstance().getOutputManager().debug("\nIndex for %s already present in the Orient schema.\n", currentVertexType.getName());
          }
          iteration++;
          statistics.wroteIndexes++;
        }
      } catch (OException e) {
        String mess = "";
        OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
        OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
        throw new OTeleporterRuntimeException(e);
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      orientGraph.shutdown();

      success = true;

    }
    else {
      OTeleporterContext.getInstance().getOutputManager().error("Changes on entities involved in hierarchical trees detected: Teleporter cannot support these variation and neither"
              + "grant coherence between the two databases. Rebuild the schema from scratch.\n");
      throw new OTeleporterRuntimeException();
    }

    return success;
  }


  /**
   *
   *
   * @param orientGraph
   * @param currentElementType
   * @return
   */
  private boolean checkAndUpdateClass(OrientBaseGraph orientGraph, OElementType currentElementType, ODriverDataTypeHandler handler) {

    boolean updated = false;
    OrientElementType orientElementType = null;

    if(currentElementType instanceof OVertexType) {
      orientElementType = orientGraph.getVertexType(currentElementType.getName());
    }
    else if(currentElementType instanceof OEdgeType) {
      orientElementType = orientGraph.getEdgeType(currentElementType.getName());
    }
    else {
      OTeleporterContext.getInstance().getOutputManager()
              .error("Fatal error: current element type '%s' is not instance neither of Vertex Type nor of EdgeType.\n", currentElementType.getName());
      throw new OTeleporterRuntimeException();
    }

    OProperty orientSchemaProperty;
    OType actualOrientType;   // the actual type present in the orientdb schema from last execution
    OType newResolvedType;    // the type returned by the resolver on the basis of the actual source

    /**
     * Class name comparison
     */
    String className = currentElementType.getName();
    if(!orientElementType.getName().equals(className)) {
      orientElementType.setName(className);
    }

    /**
     * Properties comparison
     */

    // checking from model properties
    Iterator<OModelProperty> it1 = currentElementType.getProperties().iterator();
    OModelProperty currentProperty;
    while(it1.hasNext()) {
      currentProperty = it1.next();
      orientSchemaProperty = orientElementType.getProperty(currentProperty.getName());
      newResolvedType = handler.resolveType(currentProperty.getOriginalType().toLowerCase(Locale.ENGLISH));

      if(orientSchemaProperty != null) {
        // property present in orientdb schema, check if is it equal (type check), in case it's modified
        actualOrientType = orientSchemaProperty.getType();

        // if types are not equal the property will be dropped and added again with the correct type
        if(!actualOrientType.toString().equalsIgnoreCase(newResolvedType.toString())) {
          orientElementType.dropProperty(currentProperty.getName());
          orientElementType.createProperty(currentProperty.getName(), newResolvedType);
        }
      }
      else {
        // property not present in orientdb schema, then it's added (if type allows it)
        orientElementType.createProperty(currentProperty.getName(), newResolvedType);
        updated = true;
      }
    }

    // checking from orientdb schema properties
    OProperty orientSchemaProperty2;
    Iterator<OProperty> it2 = orientElementType.declaredProperties().iterator();
    List<String> toDrop = new LinkedList<String>();
    while(it2.hasNext()) {
      orientSchemaProperty2 = it2.next();
      // if the property is not present in the model vertex type, then is added to a "to-drop list"
      if(currentElementType.getPropertyByName(orientSchemaProperty2.getName()) == null ||
              !currentElementType.getPropertyByName(orientSchemaProperty2.getName()).isIncludedInMigration()) {
        toDrop.add(orientSchemaProperty2.getName());
        updated = true;
      }
    }

    // dropping properties
    for(String propertyName: toDrop) {
      orientElementType.dropProperty(propertyName);
    }

    return updated;
  }

  public boolean inheritanceChangesPresent(OGraphModel graphModel, OrientBaseGraph orientGraph) {

    OrientVertexType orientCorrespondentVertexType;

    for(OVertexType currentVertexType: graphModel.getVerticesType()) {

      orientCorrespondentVertexType = orientGraph.getVertexType(currentVertexType.getName());

      // check for changes if vertex type is already present in the orient schema
      if(currentVertexType != null && orientCorrespondentVertexType != null) {

        if( (currentVertexType.getParentType() == null && !orientCorrespondentVertexType.getSuperClass().getName().equals("V"))
                || (currentVertexType.getParentType() != null && orientCorrespondentVertexType.getSuperClass().getName().equals("V")) )
          return true;

        else if(currentVertexType.getParentType() != null && !orientCorrespondentVertexType.getSuperClass().getName().equals("V")) {
          if(!currentVertexType.getParentType().getName().equals(orientCorrespondentVertexType.getSuperClass().getName()))
            return true;
        }
      }

    }

    return false;

  }

  public OType resolveOrientDBType(String orientdbTypeName) {
    orientdbTypeName = orientdbTypeName.toLowerCase();
    return this.orientdbTypeName2orientdbType.get(orientdbTypeName);
  }

}
