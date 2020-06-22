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

package com.orientechnologies.teleporter.mapper;

import com.orientechnologies.teleporter.model.graphmodel.OGraphModel;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;

/**
 * Interface that manages the data source schema and the destination graph model with their
 * correspondences. It has the responsibility to build in memory the two models: the first is built
 * from the source meta-data, the second from the data source schema just created.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public abstract class OSource2GraphMapper {

  // graph model
  protected OGraphModel graphModel;

  public OSource2GraphMapper() {}

  public OGraphModel getGraphModel() {
    return this.graphModel;
  }

  public abstract void buildSourceDatabaseSchema();

  public abstract void buildGraphModel(ONameResolver nameResolver);

  public abstract void applyImportConfiguration();

  public abstract String toString();
}
