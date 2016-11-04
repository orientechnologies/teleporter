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

    for (Map.Entry<String, String> entry : OServerMain.server().getAvailableStorageNames().entrySet()) {
      if (entry.getValue().startsWith("plocal:")) {

        final String path = entry.getValue().substring("plocal:".length());
        final File file = new File(path + "/teleporter-config/migration-config.json");
        if (file.exists()) {
          try {
            final ODocument doc = new ODocument().fromJSON(OIOUtils.readFileAsString(file), "noMap");

            entities.put(entry.getKey(), new OConfigurationHandler(true).buildConfigurationFromJSONDoc(doc));

            dataSources.put(entry.getKey(), new OMDMDataSources(path));

          } catch (IOException e) {
            throw OException.wrapException(new OConfigurationException("Error on reading Migration config file at: " + file), e);
          }
        }
      }
    }
  }

  public OConfiguredVertexClass getVertexClass(final String databaseName, final String vertexClassName) {
    return entities.get(databaseName).getVertexClassByName(vertexClassName);
  }

  public OConfiguredEdgeClass getEdgeClass(final String databaseName, final String edgeClassName) {
    return entities.get(databaseName).getEdgeClassByName(edgeClassName);
  }

  public OConfiguration getEntities(final String databaseName) {
    return entities.get(databaseName);
  }
  public OMDMDataSources getDataSources(final String databaseName) {
    return dataSources.get(databaseName);
  }
}
