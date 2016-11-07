package com.orientechnologies.teleporter.mdm;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.teleporter.configuration.OConfigurationHandler;
import com.orientechnologies.teleporter.configuration.api.OConfiguration;
import com.orientechnologies.teleporter.configuration.api.OConfiguredEdgeClass;
import com.orientechnologies.teleporter.configuration.api.OConfiguredVertexClass;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Luca Garulli
 */
public class OMDMConfiguration {
  // TODO: SPEED UP PARSING BY BUILDING AD-HOC POJO PARSED ONLY THE FIRST TIME
  private final Map<String, OConfiguration>  entities    = new ConcurrentHashMap<String, OConfiguration>();
  private final Map<String, OMDMDataSources> dataSources = new ConcurrentHashMap<String, OMDMDataSources>();

  public OMDMConfiguration() {

    final String dbDirectory = OServerMain.server().getDatabaseDirectory();

    for (Map.Entry<String, String> entry : OServerMain.server().getAvailableStorageNames().entrySet()) {
      final File file = new File(dbDirectory + "/" + entry.getKey() + "/teleporter-config/migration-config.json");
      if (file.exists()) {
        try {
          final ODocument doc = new ODocument().fromJSON(OIOUtils.readFileAsString(file), "noMap");

          entities.put(entry.getKey(), new OConfigurationHandler(true).buildConfigurationFromJSONDoc(doc));

          dataSources.put(entry.getKey(), new OMDMDataSources(dbDirectory + "/" + entry.getKey()));

        } catch (IOException e) {
          throw OException.wrapException(new OConfigurationException("Error on reading Migration config file at: " + file), e);
        }
      }
    }
  }

  public OConfiguredVertexClass getVertexClass(final String databaseName, final String vertexClassName) {
    final OConfiguration dbCfg = entities.get(databaseName);
    if (dbCfg != null)
      return dbCfg.getVertexClassByName(vertexClassName);
    return null;
  }

  public OConfiguredEdgeClass getEdgeClass(final String databaseName, final String edgeClassName) {
    final OConfiguration dbCfg = entities.get(databaseName);
    if (dbCfg != null)
      return dbCfg.getEdgeClassByName(edgeClassName);
    return null;
  }

  public OConfiguration getEntities(final String databaseName) {
    return entities.get(databaseName);
  }

  public OMDMDataSources getDataSources(final String databaseName) {
    return dataSources.get(databaseName);
  }

  public List<OConfiguredEdgeClass> getEdgeClasses(final String databaseName) {
    final OConfiguration dbCfg = entities.get(databaseName);
    if (dbCfg != null)
      return dbCfg.getConfiguredEdges();
    return new ArrayList<OConfiguredEdgeClass>();
  }
}
