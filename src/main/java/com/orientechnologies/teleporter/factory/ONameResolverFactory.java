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

package com.orientechnologies.teleporter.factory;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;
import com.orientechnologies.teleporter.nameresolver.OOriginalConventionNameResolver;

/**
 * Factory used to instantiate a specific NameResolver starting from its name. If the name is not
 * specified (null value) a JavaConventionNameResolver is instantiated.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class ONameResolverFactory {

  public ONameResolver buildNameResolver(String nameResolverConvention) {
    ONameResolver nameResolver = null;

    if (nameResolverConvention == null) {
      nameResolver = new OOriginalConventionNameResolver();
    } else {
      switch (nameResolverConvention) {
        case "java":
          nameResolver = new OJavaConventionNameResolver();
          break;

        case "original":
          nameResolver = new OOriginalConventionNameResolver();
          break;

        default:
          nameResolver = new OOriginalConventionNameResolver();
          OTeleporterContext.getInstance()
              .getStatistics()
              .warningMessages
              .add(
                  "Name resolver convention '"
                      + nameResolverConvention
                      + "' not found, the original name convention will be adopted.");
          break;
      }
    }

    OTeleporterContext.getInstance().setNameResolver(nameResolver);

    return nameResolver;
  }
}
