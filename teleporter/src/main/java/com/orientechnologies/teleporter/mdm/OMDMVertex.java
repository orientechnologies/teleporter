package com.orientechnologies.teleporter.mdm;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.teleporter.configuration.api.OConfiguredEdgeClass;
import com.orientechnologies.teleporter.configuration.api.OEdgeMappingInformation;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Orient Graph Vertex extension to execute a run-time join to retrieve the edges.
 *
 * @author Luca Garulli
 */
public class OMDMVertex extends OrientVertex {
  public OMDMVertex() {
  }

  public OMDMVertex(final OMDMGraphNoTx graph, final String className, final Object... fields) {
    super(graph, className, fields);
  }

  public OMDMVertex(final OMDMGraphNoTx graph, final OIdentifiable record) {
    super(graph, record);
  }

  @Override
  public Iterable<Vertex> getVertices(final Direction iDirection, final String... iLabels) {
    if (iLabels != null && iLabels.length > 0) {
      final OMDMGraphNoTx g = (OMDMGraphNoTx) getGraph();
      final OConfiguredEdgeClass cls = g.getConfiguration().getEdgeClass(g.getRawGraph().getName(), iLabels[0]);
      if (cls != null) {

        for (OEdgeMappingInformation m : cls.getMappings()) {
          final Object joinValue = getProperty(m.getFromProperty());

          final String sqlTo = "select from " + m.getToClass() + " where " + m.getToProperty() + " = ?";
          return g.command(new OCommandSQL(sqlTo)).execute(joinValue);
        }
      }
    }

    return super.getVertices(iDirection, iLabels);
  }
}
