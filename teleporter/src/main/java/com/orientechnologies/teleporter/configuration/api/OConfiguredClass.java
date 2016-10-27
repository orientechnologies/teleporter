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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 *
 */

public class OConfiguredClass {

  private String                                 name;                                                                    // mandatory
  private final Map<String, OConfiguredProperty> configuredProperties = new LinkedHashMap<String, OConfiguredProperty>(); // mandatory

  public OConfiguredClass(String elementName) {
    this.name = elementName;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Collection<OConfiguredProperty> getConfiguredProperties() {
    return this.configuredProperties.values();
  }

  public void setConfiguredProperties(final List<OConfiguredProperty> configuredProperties) {
    for (OConfiguredProperty p : configuredProperties)
      this.configuredProperties.put(p.getPropertyName(), p);
  }

  public OConfiguredProperty getProperty(final String propertyName) {
    return configuredProperties.get(propertyName);
  }

  public OConfiguredProperty getPropertyByAttribute(String attributeName) {

    for(Map.Entry<String, OConfiguredProperty> entry: configuredProperties.entrySet()) {
      OConfiguredPropertyMapping mapping = entry.getValue().getPropertyMapping();
      if(mapping != null) {
        if(mapping.getColumnName().equals(attributeName)) {
          return entry.getValue();
        }
      }
    }
    return null;
  }
}
