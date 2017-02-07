package com.orientechnologies.teleporter.model.dbschema;

import java.util.List;

/**
 * It represents the relationship between two entities.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public abstract class ORelationship {

  protected OEntity foreignEntity;        // Entity importing the key (starting entity)
  protected OEntity parentEntity;            // Entity exporting the key (arrival entity)
  protected String  direction;                    // represents the direction of the relationship

  public OEntity getForeignEntity() {
    return this.foreignEntity;
  }

  public void setForeignEntity(OEntity foreignEntity) {
    this.foreignEntity = foreignEntity;
  }

  public OEntity getParentEntity() {
    return this.parentEntity;
  }

  public void setParentEntity(OEntity parentEntity) {
    this.parentEntity = parentEntity;
  }

  public String getDirection() {
    return this.direction;
  }

  public void setDirection(String direction) {
    this.direction = direction;
  }

  public abstract List<OAttribute> getFromColumns();

  public abstract List<OAttribute> getToColumns();


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((foreignEntity == null) ? 0 : foreignEntity.hashCode());
    result = prime * result + ((parentEntity == null) ? 0 : parentEntity.hashCode());
    return result;
  }
}
