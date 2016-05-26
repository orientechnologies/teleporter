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

package com.orientdb.teleporter.test.rdbms.resolver;

import com.orientdb.teleporter.nameresolver.OJavaConventionNameResolver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class JavaNameResolverTest {


  private OJavaConventionNameResolver nameResolver;

  @Before
  public void init() {
    this.nameResolver = new OJavaConventionNameResolver();
  }


  @Test

  /*
   * Resolve Vertex Name (Java Class Convention)
   */

  public void test1() {

    String candidateName = "";
    String newCandidateName = "";

    // No white space nor underscore

    candidateName = "testClass";		// NOT acceptable (one or more uppercase char, except the first one)
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Testclass";		// acceptable (one or more uppercase char, the first one included)
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(candidateName));

    candidateName = "TestClass";		// acceptable (one or more uppercase char, except the first one)
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(candidateName));


    candidateName = "testclass";		// NOT acceptable (no uppercase chars)
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("Testclass", newCandidateName);

    candidateName = "TESTCLASS";		//  NOT acceptable (no lowercase chars)
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("Testclass", newCandidateName);

    // White space

    candidateName = "test Class";		//  NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Test class";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Test Class";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "test class";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "TEST CLASS";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);


    // Underscore

    candidateName = "test_Class";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Test_class";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Test_Class";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "test_class";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "TEST_CLASS";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

  }


  @Test

  /*
   * Resolve VertexProperty (Java Variable Convention)
   */

  public void test2() {

    String candidateName = "";
    String newCandidateName = "";

    // No white space nor underscore

    candidateName = "testVariable";		// acceptable (one or more uppercase char, except the first one)
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(candidateName));

    candidateName = "Testvariable";		// NOT acceptable (one or more uppercase char, the first one included)
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testvariable", newCandidateName);

    candidateName = "TestVariable";		// NOT acceptable (one or more uppercase char, except the first one)
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "testvariable";		// acceptable (no uppercase chars)
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(candidateName));

    candidateName = "TESTVARIABLE";		// NOT acceptable (no lowercase chars)
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testvariable", newCandidateName);

    // White space

    candidateName = "test Variable";		//  NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "Test variable";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "Test Variable";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "test variable";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "TEST VARIABLE";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);


    // Underscore

    candidateName = "test_Variable";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "Test_variable";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "Test_Variable";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "test_variable";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "TEST_VARIABLE";		// NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

  }
}
