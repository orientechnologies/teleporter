package com.orientechnologies.teleporter.mdm;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;

/**
 * Orient Graph Edge extension.
 *
 * @author Luca Garulli
 */
public class OMDMEdge extends OrientEdge {
  public OMDMEdge() {
  }

  public OMDMEdge(OrientBaseGraph rawGraph, OIdentifiable rawEdge) {
    super(rawGraph, rawEdge);
  }

  public OMDMEdge(OrientBaseGraph rawGraph, OIdentifiable rawEdge, String iLabel) {
    super(rawGraph, rawEdge, iLabel);
  }

  public OMDMEdge(OrientBaseGraph rawGraph, String iLabel, Object... fields) {
    super(rawGraph, iLabel, fields);
  }

  public OMDMEdge(OrientBaseGraph rawGraph, OIdentifiable out, OIdentifiable in, String iLabel) {
    super(rawGraph, out, in, iLabel);
  }
}
