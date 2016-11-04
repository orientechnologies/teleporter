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
          final StringBuilder sqlTo = new StringBuilder("select from " + m.getToClass() + " where ");
          final String[] properties = m.getFromProperties();
          final Object[] joinValues = new Object[properties.length];
          for (int i = 0; i < properties.length; ++i) {
            final String p = properties[i];
            joinValues[i] = getProperty(p);

            if (i > 0)
              sqlTo.append(" and ");

            sqlTo.append(m.getToProperty() + " = ?");
          }

          return g.command(new OCommandSQL(sqlTo.toString())).execute(joinValues);
        }
      }
    }

    return super.getVertices(iDirection, iLabels);
  }
}
