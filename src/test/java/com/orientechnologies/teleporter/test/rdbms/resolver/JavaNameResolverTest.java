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

package com.orientechnologies.teleporter.test.rdbms.resolver;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class JavaNameResolverTest {

  private OJavaConventionNameResolver nameResolver;

  @Before
  public void init() {
    this.nameResolver = new OJavaConventionNameResolver();
  }

  @Test

  /*
   * Resolve OVertex Name (Java Class Convention)
   */

  public void test1() {

    String candidateName = "";
    String newCandidateName = "";

    // No white space nor underscore

    candidateName =
        "testClass"; // NOT acceptable (one or more uppercase char, except the first one)
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Testclass"; // acceptable (one or more uppercase char, the first one included)
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(candidateName));

    candidateName = "TestClass"; // acceptable (one or more uppercase char, except the first one)
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(candidateName));

    candidateName = "testclass"; // NOT acceptable (no uppercase chars)
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("Testclass", newCandidateName);

    candidateName = "TESTCLASS"; //  NOT acceptable (no lowercase chars)
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("Testclass", newCandidateName);

    // White space

    candidateName = "test Class"; //  NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Test class"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Test Class"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "test class"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "TEST CLASS"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    // Underscore

    candidateName = "test_Class"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Test_class"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "Test_Class"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "test_class"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName = "TEST_CLASS"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);
  }

  @Test

  /*
   * Resolve OVertex Name (Java Class Convention)
   * Test-Fix: Resolver supports names ending with '_'.
   */

  public void NamesEndingWithUnderscore() {

    String candidateName = "";
    String newCandidateName = "";

    // No white space nor underscore

    candidateName =
        "testClass_"; // NOT acceptable (one or more uppercase char, except the first one)
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.toJavaClassConvention(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaClassConvention(newCandidateName));
    assertEquals("TestClass", newCandidateName);

    candidateName =
        "test_class_"; // NOT acceptable (one or more uppercase char, except the first one)
    assertEquals(false, nameResolver.isCompliantToJavaClassConvention(candidateName));
    newCandidateName = nameResolver.toJavaClassConvention(candidateName);
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

    candidateName = "testVariable"; // acceptable (one or more uppercase char, except the first one)
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(candidateName));

    candidateName =
        "Testvariable"; // NOT acceptable (one or more uppercase char, the first one included)
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testvariable", newCandidateName);

    candidateName =
        "TestVariable"; // NOT acceptable (one or more uppercase char, except the first one)
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "testvariable"; // acceptable (no uppercase chars)
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(candidateName));

    candidateName = "TESTVARIABLE"; // NOT acceptable (no lowercase chars)
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testvariable", newCandidateName);

    // White space

    candidateName = "test Variable"; //  NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "Test variable"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "Test Variable"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "test variable"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "TEST VARIABLE"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    // Underscore

    candidateName = "test_Variable"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "Test_variable"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "Test_Variable"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "test_variable"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);

    candidateName = "TEST_VARIABLE"; // NOT acceptable
    assertEquals(false, nameResolver.isCompliantToJavaVariableConvention(candidateName));
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals(true, nameResolver.isCompliantToJavaVariableConvention(newCandidateName));
    assertEquals("testVariable", newCandidateName);
  }
}
