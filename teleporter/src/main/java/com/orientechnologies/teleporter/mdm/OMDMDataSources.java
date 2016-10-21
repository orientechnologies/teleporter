package com.orientechnologies.teleporter.mdm;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.util.ODriverConfigurator;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Luca Garulli
 */
public class OMDMDataSources {
  private final Map<String, ODocument> sources = new ConcurrentHashMap<String, ODocument>();
  private Connection                   connection;

  public OMDMDataSources(final String path) {

    final File file = new File(path + "/teleporter-config/sources-access-info.json");
    if (file.exists()) {
      try {
        final ODocument doc = new ODocument().fromJSON(OIOUtils.readFileAsString(file), "noMap");
        final List<ODocument> s = doc.field("sources");

        for (ODocument d : s) {
          for (String f : d.fieldNames()) {
            sources.put(f, (ODocument) d.field(f));
          }
        }

      } catch (IOException e) {
        throw OException.wrapException(new OConfigurationException("Error on reading Source config file at: " + file), e);
      }
    }
  }

  public String getUserName(final String sourceName) {
    return getSource(sourceName).field("username");
  }

  public String getUserPassword(final String sourceName) {
    return getSource(sourceName).field("password");
  }

  public String getDriverName(final String sourceName) {
    return getSource(sourceName).field("driverName");
  }

  public String getURL(final String sourceName) {
    return getSource(sourceName).field("url");
  }

  private ODocument getSource(String sourceName) {
    final ODocument source = sources.get(sourceName);
    if (source == null)
      throw new IllegalArgumentException("source name is not configured");
    return source;
  }

  public Connection getConnection(final String source) throws SQLException, ClassNotFoundException {
    if (connection == null) {
      new ODriverConfigurator().checkDriverConfiguration(source);

      Class.forName(getDriverName(source));

      connection = DriverManager.getConnection(getURL(source), getUserName(source), getUserPassword(source));
    }
    return connection;
  }
}
