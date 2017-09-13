/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.teleporter.configuration.api;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OConfiguredClass {

  protected String name;                                                  // mandatory
  protected final Map<String, OConfiguredProperty> configuredProperties = new LinkedHashMap<String, OConfiguredProperty>();         // mandatory
  protected OConfiguration globalConfiguration;

  public OConfiguredClass(String elementName, OConfiguration globalConfiguration) {
    this.name = elementName;
    this.globalConfiguration = globalConfiguration;
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

  public OConfiguration getGlobalConfiguration() {
    return this.globalConfiguration;
  }

  public void setGlobalConfiguration(OConfiguration globalConfiguration) {
    this.globalConfiguration = globalConfiguration;
  }

  public OConfiguredProperty getProperty(final String propertyName) {
    return configuredProperties.get(propertyName);
  }

  public OConfiguredProperty getPropertyByAttribute(String attributeName) {

    for (Map.Entry<String, OConfiguredProperty> entry : configuredProperties.entrySet()) {
      OConfiguredPropertyMapping mapping = entry.getValue().getPropertyMapping();
      if (mapping != null) {
        if (mapping.getColumnName().equals(attributeName)) {
          return entry.getValue();
        }
      }
    }
    return null;
  }

  public String[] getPropertiesByColumns(List<String> columns) {
    String[] properties = new String[columns.size()];
    int i = 0;
    int j = 0;

    // maintains properties order (otherwise index does not work)
    for (Map.Entry entry : this.configuredProperties.entrySet()) {
      OConfiguredProperty currConfiguredProperty = (OConfiguredProperty) entry.getValue();
      if (columns.contains(currConfiguredProperty.getPropertyMapping().getColumnName())) {
        properties[j] = currConfiguredProperty.getPropertyName();
        j++;
      }
      i++;
      if (j >= columns.size()) {
        break;
      }
    }
    return properties;
  }
}
