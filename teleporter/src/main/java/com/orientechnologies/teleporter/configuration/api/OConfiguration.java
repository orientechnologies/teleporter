/*
 * Copyright 2016 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.teleporter.configuration.api;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.model.dbschema.OEntity;

import java.util.LinkedList;
import java.util.List;

/**
 * It collects all the information contained in the migrationConfigDoc submitted by the user.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 *
 */

public class OConfiguration {

    private static OConfiguration instance = null;

    private List<OConfiguredVertexClass> configuredVertices;  // may be empty but not null
    private List<OConfiguredEdgeClass> configuredEdges;       // may be empty but not null

    public OConfiguration() {
        this.configuredVertices = new LinkedList<OConfiguredVertexClass>();
        this.configuredEdges = new LinkedList<OConfiguredEdgeClass>();
    }

    public static OConfiguration getInstance() {
        if (instance == null) {
            instance = new OConfiguration();
        }
        return instance;
    }

    public List<OConfiguredVertexClass> getConfiguredVertices() {
        return this.configuredVertices;
    }

    public void setConfiguredVertices(List<OConfiguredVertexClass> configuredVertices) {
        this.configuredVertices = configuredVertices;
    }

    public List<OConfiguredEdgeClass> getConfiguredEdges() {
        return this.configuredEdges;
    }

    public void setConfiguredEdges(List<OConfiguredEdgeClass> configuredEdges) {
        this.configuredEdges = configuredEdges;
    }

    public OConfiguredVertexClass getVertexClassByName(String vertexClassName) {

        for(OConfiguredVertexClass currVertexClass: this.configuredVertices) {
            if(currVertexClass.getName().equals(vertexClassName)) {
                return currVertexClass;
            }
        }
        return null;
    }

    public OConfiguredEdgeClass getEdgeClassByName(String edgeClassName) {

        for(OConfiguredEdgeClass currEdgeClass: this.configuredEdges) {
            if(currEdgeClass.getName().equals(edgeClassName)) {
                return currEdgeClass;
            }
        }
        return null;
    }

    public OConfiguredVertexClass getVertexByMappedEntities(List<OEntity> mappedEntities) {

        for(OConfiguredVertexClass currConfiguredVertex: this.configuredVertices) {
            boolean isTargetVertex = this.isTargetVertex(currConfiguredVertex, mappedEntities);

            if(isTargetVertex) {
                return currConfiguredVertex;
            }
        }
        return null;
    }

    private boolean isTargetVertex(OConfiguredVertexClass currConfiguredVertex, List<OEntity> mappedEntities) {

        List<OSourceTable> sourceTables = currConfiguredVertex.getMapping().getSourceTables();

        for(OEntity currEntity: mappedEntities) {
            String entityName = currEntity.getName();

            boolean containsEntity = false;
            for (OSourceTable sourceTable : sourceTables) {
                if(sourceTable.getTableName().equals(entityName)) {
                    containsEntity = true;
                    break;
                }
            }
            if(!containsEntity) {
                return false;
            }
        }

        return true;
    }

    public OConfiguredVertexClass getVertexClassByTableName(String tableName) {

        for(OConfiguredVertexClass vertexClass: this.configuredVertices) {
            List<OSourceTable> sourceTables = vertexClass.getMapping().getSourceTables();
            for(OSourceTable sourceTable: sourceTables) {
                if(sourceTable.getTableName().equals(tableName)) {
                    return vertexClass;
                }
            }
        }
        return null;
    }
}
