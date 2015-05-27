package com.orientechnologies.orient.drakkar.main;

/**
 * 
 */
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * @author gabriele
 *
 */
public class DescProperty {
  
  public static void main(String[] args) {
    
    OrientGraphNoTx orientGraph = new OrientGraphNoTx("plocal:/home/gabriele/orientdb-community-2.1-rc3/databases/provaDesc");
    OrientVertexType vertextType = orientGraph.createVertexType("Clazz");
    vertextType.createProperty("desc", OType.STRING);

  }

}
