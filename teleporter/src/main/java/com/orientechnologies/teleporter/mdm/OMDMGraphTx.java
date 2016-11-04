package com.orientechnologies.teleporter.mdm;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Orient Graph Tx extension to execute a run-time join to retrieve the edges.
 *
 * @author Luca Garulli
 */
public class OMDMGraphTx extends OrientGraph {
  private final OMDMConfiguration configuration;

  public OMDMGraphTx(final OMDMConfiguration configuration, final ODatabaseDocumentTx db) {
    super(db);
    this.configuration = configuration;
  }

  public OMDMGraphTx(final OMDMConfiguration configuration, final String url) {
    super(url);
    this.configuration = configuration;
  }

  public OMDMGraphTx(final OMDMConfiguration configuration, final String url, final String username, final String password) {
    super(url, username, password);
    this.configuration = configuration;
  }

  public OMDMGraphTx(final OMDMConfiguration configuration, final ODatabaseDocumentTx iDatabase, final String user,
      final String password, final Settings iConfiguration) {
    super(iDatabase, user, password, iConfiguration);
    this.configuration = configuration;
  }

  public OMDMGraphTx(final OMDMConfiguration configuration, final OPartitionedDatabasePool pool, final Settings settings) {
    super(pool, settings);
    this.configuration = configuration;
  }

  public OMDMConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  protected OrientVertex getVertexInstance(final String className, final Object... fields) {
    return new OMDMVertex(this, className, fields);
  }

  @Override
  protected OrientVertex getVertexInstance(final OIdentifiable id) {
    return new OMDMVertex(this, id);
  }
}
