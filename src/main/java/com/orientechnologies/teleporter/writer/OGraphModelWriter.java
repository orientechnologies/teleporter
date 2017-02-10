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
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.context.OTeleporterStatistics;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.model.dbschema.OAttribute;
import com.orientechnologies.teleporter.model.dbschema.ODataBaseSchema;
import com.orientechnologies.teleporter.model.dbschema.OLogicalRelationship;
import com.orientechnologies.teleporter.model.graphmodel.*;
import com.orientechnologies.teleporter.persistence.handler.ODriverDataTypeHandler;

import java.util.*;

/**
 * Writer that has the responsibility to write the model of the destination Orient Graph
 * on OrientDB as an OrientDB Schema.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OGraphModelWriter {

  private Map<String, OType> orientdbTypeName2orientdbType;

  private OConfiguration     previousConfiguration;

  public OGraphModelWriter() {
    this.init();
  }

  public OGraphModelWriter(OConfiguration previousConfig) {
    this.previousConfiguration = previousConfig;
    this.init();
  }

  private void init() {
    this.orientdbTypeName2orientdbType = new HashMap<String, OType>();

    this.orientdbTypeName2orientdbType.put("boolean", OType.BOOLEAN);
    this.orientdbTypeName2orientdbType.put("integer", OType.INTEGER);
    this.orientdbTypeName2orientdbType.put("decimal", OType.DECIMAL);
    this.orientdbTypeName2orientdbType.put("short", OType.SHORT);
    this.orientdbTypeName2orientdbType.put("long", OType.LONG);
    this.orientdbTypeName2orientdbType.put("float", OType.FLOAT);
    this.orientdbTypeName2orientdbType.put("double", OType.DOUBLE);
    this.orientdbTypeName2orientdbType.put("datetime", OType.DATETIME);
    this.orientdbTypeName2orientdbType.put("date", OType.DATE);
    this.orientdbTypeName2orientdbType.put("string", OType.STRING);
    this.orientdbTypeName2orientdbType.put("binary", OType.BINARY);
    this.orientdbTypeName2orientdbType.put("byte", OType.BYTE);
  }

  public boolean writeModelOnOrient(OER2GraphMapper mapper, ODriverDataTypeHandler handler, String outParentDatabaseDirectory, String dbName) {

    boolean success = false;
    OGraphModel graphModel = mapper.getGraphModel();

    ODatabaseDocument orientGraph;

    try {

      // starting OrientDB instance

      OTeleporterContext.getInstance().initOrientDBInstance(outParentDatabaseDirectory);
      if(! OTeleporterContext.getInstance().getOrientDBInstance().exists(dbName)) {
        OTeleporterContext.getInstance().getOrientDBInstance().create(dbName, ODatabaseType.PLOCAL);
      }
      orientGraph = OTeleporterContext.getInstance().getOrientDBInstance().open(dbName,"admin","admin");
    } catch (Exception e) {
      String mess = "";
      OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
      OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new OTeleporterRuntimeException(e);
    }

    OTeleporterStatistics statistics = OTeleporterContext.getInstance().getStatistics();
    statistics.startWork3Time = new Date();
    statistics.runningStepNumber = 3;

    // orient graph schema
    OSchema orientSchema = orientGraph.getMetadata().getSchema();

    int numberOfVertices = graphModel.getVerticesType().size();
    statistics.totalNumberOfVertexTypes = numberOfVertices;
    int numberOfEdges = graphModel.getEdgesType().size();
    statistics.totalNumberOfEdgeTypes = numberOfEdges;
    statistics.totalNumberOfIndices = numberOfVertices;

    // deleting orient classes not present in the current graph model
    Collection<OClass> orientClasses = orientSchema.getClasses();
    for (OClass currOrientClass : orientClasses) {
      String orientClassName = currOrientClass.getName();
      if (!(orientClassName.startsWith("O") || orientClassName.startsWith("V") || orientClassName.startsWith("E") || orientClassName
          .startsWith("_"))) {
        if (graphModel.getVertexTypeByNameIgnoreCase(orientClassName) == null
            && graphModel.getEdgeTypeByNameIgnoreCase(orientClassName) == null) {
          orientSchema.dropClass(orientClassName);
        }
      }
    }

    if (!this.inheritanceChangesPresent(graphModel, orientGraph)) {

      try {

        /*
         * Writing vertex-type
         */

        if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
          OTeleporterContext.getInstance().getOutputManager().debug("\nWriting vertex-types on OrientDB Schema...\n");
        }

        String statement;
        OCommandSQL sqlCommand;
        OType type;
        Iterator<OModelProperty> it = null;

        int iteration = 1;
        for (OVertexType currentVertexType : graphModel.getVerticesType()) {

          if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
            OTeleporterContext.getInstance().getOutputManager()
                .debug("\nWriting '%s' vertex-type  (%s/%s)...\n", currentVertexType.getName(), iteration, numberOfVertices);
          }

          // check if vertex type is already present in the orient schema
          OClass newVertexType = orientGraph.getClass(currentVertexType.getName());

          if (newVertexType == null) {

            // inheritance case
            OElementType parentType = currentVertexType.getParentType();
            if (parentType != null) {
              OClass superClass = orientGraph.createClassIfNotExist(parentType.getName(), "V");
              newVertexType = orientGraph.createClass(currentVertexType.getName(), superClass.getName());
            }
            else
              newVertexType = orientGraph.createVertexClass(currentVertexType.getName());

            OModelProperty currentProperty = null;
            it = currentVertexType.getProperties().iterator();

            while (it.hasNext()) {
              currentProperty = it.next();
              if (currentProperty.isIncludedInMigration()) {
                if (currentProperty.getOrientdbType() == null) {
                  type = handler.resolveType(currentProperty.getOriginalType().toLowerCase(Locale.ENGLISH));
                } else {
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
                  statistics.warningMessages.add(
                      currentProperty.getOriginalType() + " type is not supported, the correspondent property will be dropped.");
                }
              }
            }
            if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
              OTeleporterContext.getInstance().getOutputManager().debug("\nVertex-type '%s' wrote.\n", currentVertexType.getName());
            }
          } else {
            boolean updated = this.checkAndUpdateClass(orientGraph, currentVertexType, handler);

            if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
              if (updated) {
                OTeleporterContext.getInstance().getOutputManager().debug("\nVertex-type '%s' updated.\n", currentVertexType.getName());
              } else {
                OTeleporterContext.getInstance().getOutputManager()
                    .debug("\nVertex-type '%s' already present in the Orient schema.\n", currentVertexType.getName());
              }
            }
          }

          iteration++;
          statistics.wroteVertexType++;
        }

        /*
         * Writing edge-type
         */

        if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
          OTeleporterContext.getInstance().getOutputManager().debug("\nWriting edge-types on OrientDB Schema...\n");
        }

        OClass newEdgeType;

        iteration = 1;
        for (OEdgeType currentEdgeType : graphModel.getEdgesType()) {

          if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
            OTeleporterContext.getInstance().getOutputManager()
                .debug("\nWriting '%s' edge-type  (%s/%s)...\n", currentEdgeType.getName(), iteration, numberOfEdges);
          }

          // check if edge type is already present in the orient schema
          newEdgeType = orientGraph.getClass(currentEdgeType.getName());

          if (newEdgeType == null) {
            newEdgeType = orientGraph.createEdgeClass(currentEdgeType.getName());
            OModelProperty currentProperty = null;
            it = currentEdgeType.getProperties().iterator();

            while (it.hasNext()) {
              currentProperty = it.next();
              if (currentProperty.isIncludedInMigration()) {
                if (currentProperty.getOrientdbType() == null) {
                  type = handler.resolveType(currentProperty.getOriginalType().toLowerCase(Locale.ENGLISH));
                } else {
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
                  statistics.warningMessages.add(
                      currentProperty.getOriginalType() + " type is not supported, the correspondent property will be dropped.");
                }
              }
            }

            if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
              OTeleporterContext.getInstance().getOutputManager().debug("\nEdge-type '%s' wrote.\n", currentEdgeType.getName());
            }
          } else {
            boolean updated = this.checkAndUpdateClass(orientGraph, currentEdgeType, handler);

            if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
              if (updated) {
                OTeleporterContext.getInstance().getOutputManager().debug("\nEdge-type '%s' updated.\n", currentEdgeType.getName());
              } else {
                OTeleporterContext.getInstance().getOutputManager()
                    .debug("\nEdge-type '%s' already present in the Orient schema.\n", currentEdgeType.getName());
              }
            }
          }
          iteration++;
          statistics.wroteEdgeType++;
        }

        /*
         *  Writing indexes on properties belonging to the original primary key
         */

        if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
          OTeleporterContext.getInstance().getOutputManager().debug("\nBuilding indexes on properties belonging to the original primary keys...\n");
        }

        String currentType = null;
        List<String> properties = null;
        iteration = 1;
        OIndexManagerProxy indexManager = (OIndexManagerProxy) orientGraph.getMetadata().getIndexManager();
        boolean isPresent;
        for (OVertexType currentVertexType : graphModel.getVerticesType()) {

          currentType = currentVertexType.getName();
          properties = new ArrayList<String>();
          for (OModelProperty currentProperty : currentVertexType.getProperties()) {
            if (currentProperty.isFromPrimaryKey()) {
              properties.add(currentProperty.getName());
            }
          }
          this.buildIndexOnExternalKey(orientGraph, numberOfVertices, iteration, currentType, properties, indexManager,
              currentVertexType);

          iteration++;
          statistics.wroteIndexes++;
        }


        /*
         *  Writing indexes on properties involved in Logical Relationships
         */

        iteration = 1;
        ODataBaseSchema dbSchema = mapper.getDataBaseSchema();
        for (OLogicalRelationship logicalRelationship : dbSchema.getLogicalRelationships()) {

          // index on in-vertex type
          OVertexType currentInVertexType = mapper.getVertexTypeByEntity(logicalRelationship.getParentEntity());

          currentType = currentInVertexType.getName();
          properties = new ArrayList<String>();

          String indexClassName = currentType + ".";
          for (OAttribute attribute : logicalRelationship.getToColumns()) {
            String correspondentPropertyName = mapper
                .getPropertyNameByVertexTypeAndAttribute(currentInVertexType, attribute.getName());
            properties.add(correspondentPropertyName);
            indexClassName += correspondentPropertyName + "_";
          }
          indexClassName = indexClassName.substring(0, indexClassName.lastIndexOf("_"));
          this.buildLogicalIndex(orientGraph, numberOfVertices, iteration, currentType, properties, indexManager,
              currentInVertexType, indexClassName);

          // index on out-vertex type
          OVertexType currentOutVertexType = mapper.getVertexTypeByEntity(logicalRelationship.getForeignEntity());

          currentType = currentOutVertexType.getName();
          properties = new ArrayList<String>();

          indexClassName = currentType + ".";
          for (OAttribute attribute : logicalRelationship.getFromColumns()) {
            String correspondentPropertyName = mapper
                .getPropertyNameByVertexTypeAndAttribute(currentOutVertexType, attribute.getName());
            properties.add(correspondentPropertyName);
            indexClassName += correspondentPropertyName + "_";
          }
          indexClassName = indexClassName.substring(0, indexClassName.lastIndexOf("_"));
          this.buildLogicalIndex(orientGraph, numberOfVertices, iteration, currentType, properties, indexManager,
              currentInVertexType, indexClassName);
        }

      } catch (OException e) {
        String mess = "";
        OTeleporterContext.getInstance().printExceptionMessage(e, mess, "error");
        OTeleporterContext.getInstance().printExceptionStackTrace(e, "error");
        throw new OTeleporterRuntimeException(e);
      }
      statistics.notifyListeners();
      statistics.runningStepNumber = -1;
      orientGraph.close();

      success = true;

    } else {
      OTeleporterContext.getInstance().getOutputManager().error(
          "Changes on entities involved in hierarchical trees detected: Teleporter cannot support these variation and neither"
              + "grant coherence between the two databases. Rebuild the schema from scratch.\n");
      throw new OTeleporterRuntimeException();
    }

    OTeleporterContext.getInstance().getOrientDBInstance().close();

    return success;
  }

  /**
   * It builds an index on the properties correspondent to the columns belonging to the original primary key (external key).
   * If the index is already defined no more indexes will be added.
   * During the sync if the properties changed names, the old index will be dropped.
   *
   * @param orientGraph
   * @param numberOfVertices
   * @param iteration
   * @param currentType
   * @param properties
   * @param indexManager
   * @param currentVertexType
   */

  private void buildIndexOnExternalKey(ODatabaseDocument orientGraph, int numberOfVertices, int iteration, String currentType,
      List<String> properties, OIndexManagerProxy indexManager, OVertexType currentVertexType) {
    boolean isPresent;
    String statement;
    OCommandSQL sqlCommand;

    // checking if the old index is based on the same properties of the current Class, if not it will be deleted
    String indexClassName = currentType + ".pkey";
    OIndex<?> classIndex = indexManager.getClassIndex(currentType, indexClassName);
    if (classIndex != null) {
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

    if (!isPresent) {

      String propertiesList = "";
      int j = 0;
      for (String property : properties) {
        if (j == properties.size() - 1)
          propertiesList += property;
        else
          propertiesList += property + ",";
        j++;
      }

      if (!propertiesList.isEmpty()) {

        if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
          OTeleporterContext.getInstance().getOutputManager()
              .debug("\nBuilding index for '%s' on %s  (%s/%s)...\n", currentVertexType.getName(), propertiesList, iteration,
                  numberOfVertices);
        }

        statement =
            "create index `" + currentType + ".pkey`" + " on `" + currentType + "` (" + propertiesList + ") unique_hash_index";
        sqlCommand = new OCommandSQL(statement);
        orientGraph.command(sqlCommand).execute();

        if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
          OTeleporterContext.getInstance().getOutputManager().debug("\nIndex for %s built.\n", currentVertexType.getName());
        }
      } else {
        OTeleporterContext.getInstance().getStatistics().warningMessages.add(
            "The table '" + currentVertexType.getName() + "' has not primary key constraints defined in the db schema,"
                + " thus the correspondent Class Vertex in Orient will not have a default index on the property deriving from the original primary key.");
      }
    } else {
      if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
        OTeleporterContext.getInstance().getOutputManager()
            .debug("\nIndex for %s already present in the Orient schema.\n", currentVertexType.getName());
      }
    }
  }

  /**
   * It build an index if it's not already present in the database.
   *
   * @param orientGraph
   * @param numberOfVertices
   * @param iteration
   * @param currentType
   * @param properties
   * @param indexManager
   * @param currentVertexType
   * @param indexClassName
   */

  private void buildLogicalIndex(ODatabaseDocument orientGraph, int numberOfVertices, int iteration, String currentType,
      List<String> properties, OIndexManagerProxy indexManager, OVertexType currentVertexType, String indexClassName) {

    boolean isPresent;
    String statement;
    OCommandSQL sqlCommand;

    // check if vertex type is already present in the orient schema
    isPresent = indexManager.existsIndex(indexClassName);

    if (!isPresent) {

      String propertiesList = "";
      int j = 0;
      for (String property : properties) {
        if (j == properties.size() - 1)
          propertiesList += property;
        else
          propertiesList += property + ",";
        j++;
      }

      if (!propertiesList.isEmpty()) {
        if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
          OTeleporterContext.getInstance().getOutputManager()
              .debug("\nBuilding index for '%s' on %s  (%s/%s)...\n", currentVertexType.getName(), propertiesList, iteration,
                  numberOfVertices);
        }
        statement = "create index `" + indexClassName + "` on `" + currentType + "` (" + propertiesList + ") notunique_hash_index";
        sqlCommand = new OCommandSQL(statement);
        orientGraph.command(sqlCommand).execute();
        if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
          OTeleporterContext.getInstance().getOutputManager().debug("\nIndex for %s built.\n", currentVertexType.getName());
        }
      } else {
        OTeleporterContext.getInstance().getStatistics().warningMessages.add(
            "The table '" + currentVertexType.getName() + "' has not primary key constraints defined in the db schema,"
                + " thus the correspondent Class Vertex in Orient will not have a default index on the property deriving from the original primary key.");
      }
    } else {
      if(OTeleporterContext.getInstance().getOutputManager().getLevel() == OOutputStreamManager.DEBUG_LEVEL) {
        OTeleporterContext.getInstance().getOutputManager()
            .debug("\nIndex for %s already present in the Orient schema.\n", currentVertexType.getName());
      }
    }
  }

  /**
   * @param orientGraph
   * @param currentElementType
   *
   * @return
   */
  private boolean checkAndUpdateClass(ODatabaseDocument orientGraph, OElementType currentElementType,
      ODriverDataTypeHandler handler) {

    boolean updated = false;
    OClass orientElementType = null;

    if (currentElementType instanceof OVertexType) {
      orientElementType = orientGraph.getClass(currentElementType.getName());
    } else if (currentElementType instanceof OEdgeType) {
      orientElementType = orientGraph.getClass(currentElementType.getName());
    } else {
      OTeleporterContext.getInstance().getOutputManager()
          .error("Fatal error: current element type '%s' is not instance neither of Vertex Type nor of EdgeType.\n",
              currentElementType.getName());
      throw new OTeleporterRuntimeException();
    }

    OProperty orientSchemaProperty;
    OType actualOrientType;   // the actual type present in the orientdb schema from last execution
    OType newResolvedType;    // the type returned by the resolver on the basis of the actual source

    /**
     * Class name comparison
     */
    String className = currentElementType.getName();
    if (!orientElementType.getName().equals(className)) {
      orientElementType.setName(className);
    }

    /**
     * Properties comparison
     */

    // checking from model properties
    Iterator<OModelProperty> it1 = currentElementType.getProperties().iterator();
    OModelProperty currentModelProperty;
    while (it1.hasNext()) {
      currentModelProperty = it1.next();

      if (currentModelProperty.isIncludedInMigration()) {
        orientSchemaProperty = orientElementType.getProperty(currentModelProperty.getName());
        if (currentModelProperty.getOrientdbType() != null) {
          newResolvedType = this.resolveOrientDBType(currentModelProperty.getOrientdbType());
        } else {
          newResolvedType = handler.resolveType(currentModelProperty.getOriginalType().toLowerCase(Locale.ENGLISH));
        }

        if (orientSchemaProperty != null) {
          // property present in orientdb schema, check if is it equal (type check), in case it's modified
          actualOrientType = orientSchemaProperty.getType();

          // if types are not equal the property will be dropped and added again with the correct type
          if (!actualOrientType.toString().equalsIgnoreCase(newResolvedType.toString())) {
            orientElementType.dropProperty(currentModelProperty.getName());
            orientElementType.createProperty(currentModelProperty.getName(), newResolvedType);
          }
        } else {
          // property not present in orientdb schema, then it's added (if type allows it)
          orientElementType.createProperty(currentModelProperty.getName(), newResolvedType);
          updated = true;
        }
      }
    }

    // checking from orientdb schema properties
    OProperty orientSchemaProperty2;
    Iterator<OProperty> it2 = orientElementType.declaredProperties().iterator();
    List<String> toDrop = new LinkedList<String>();
    while (it2.hasNext()) {
      orientSchemaProperty2 = it2.next();
      // if the property is not present in the model vertex type, then is added to a "to-drop list"
      if (currentElementType.getPropertyByName(orientSchemaProperty2.getName()) == null || !currentElementType
          .getPropertyByName(orientSchemaProperty2.getName()).isIncludedInMigration()) {
        toDrop.add(orientSchemaProperty2.getName());
        updated = true;
      }
    }

    // dropping properties
    for (String propertyName : toDrop) {
      orientElementType.dropProperty(propertyName);
    }

    return updated;
  }

  public boolean inheritanceChangesPresent(OGraphModel graphModel, ODatabaseDocument orientGraph) {

    for (OVertexType currentVertexType : graphModel.getVerticesType()) {

      OClass orientCorrespondentVertexType = orientGraph.getClass(currentVertexType.getName());

      // check for changes if vertex type is already present in the orient schema
      if (currentVertexType != null && orientCorrespondentVertexType != null) {

        if ((currentVertexType.getParentType() == null && !orientCorrespondentVertexType.isSubClassOf("V")) || (
            currentVertexType.getParentType() != null && orientCorrespondentVertexType.isSubClassOf("V")))
          return true;

        else if (currentVertexType.getParentType() != null && !orientCorrespondentVertexType.isSubClassOf("V")) {
          if (!currentVertexType.getParentType().getName().equals(orientCorrespondentVertexType.getSuperClasses().get(0).getName())) // now we can have just a super-class for each vertex type
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
