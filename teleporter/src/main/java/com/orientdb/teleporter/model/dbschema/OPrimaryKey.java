/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
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

package com.orientdb.teleporter.model.dbschema;

/**
 * It represents a primary key for an entity.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OPrimaryKey extends OKey {


  public OPrimaryKey(OEntity belongingEntity) {
    super(belongingEntity);
  }

  public OAttribute getAttributeByOrdinalPosition(int ordinalPosition) {

    // overflow
    if(ordinalPosition > super.getInvolvedAttributes().size())
      return null;

    for(OAttribute attribute: super.involvedAttributes) {
      if(attribute.getOrdinalPosition() == ordinalPosition)
        return attribute;
    }
    return null;

  }

}
