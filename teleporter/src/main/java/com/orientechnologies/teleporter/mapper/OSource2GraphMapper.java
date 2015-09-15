/*
 * Copyright 2015 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.teleporter.mapper;

import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.model.dbschema.ODataSourceSchema;
import com.orientechnologies.teleporter.model.graphmodel.OGraphModel;
import com.orientechnologies.teleporter.nameresolver.ONameResolver;


/**
 * Interface that manages the data source schema and the destination graph model with their correspondences.
 * It has the responsibility to build in memory the two models: the first is built from the source meta-data,
 * the second from the data source schema just created.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public abstract class OSource2GraphMapper {

  // graph model
  protected OGraphModel graphModel;
  
  
  public OSource2GraphMapper() {}
  
  
  public OGraphModel getGraphModel() {
    return this.graphModel;
  }
  
  public abstract ODataSourceSchema getSourceSchema();

  public abstract void buildSourceSchema(OTeleporterContext context);

  public abstract void buildGraphModel(ONameResolver nameResolver, OTeleporterContext context);

  public abstract String toString();

}
