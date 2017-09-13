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

import com.orientechnologies.teleporter.nameresolver.OOriginalConventionNameResolver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class OriginalNameResolverTest {

  private OOriginalConventionNameResolver nameResolver;

  @Before
  public void init() {
    this.nameResolver = new OOriginalConventionNameResolver();
  }

  @Test

  /*
   * Resolve Vertex Name (Original Class Convention)
   */

  public void test1() {

    String candidateName = "";
    String newCandidateName = "";

    // No white space nor underscore

    candidateName = "testClass";    // NOT acceptable (one or more uppercase char, except the first one)
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("testClass", newCandidateName);

    candidateName = "Testclass";    // acceptable (one or more uppercase char, the first one included)
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("Testclass", newCandidateName);

    candidateName = "TestClass";    // acceptable (one or more uppercase char, except the first one)
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("TestClass", newCandidateName);

    candidateName = "testclass";    // NOT acceptable (no uppercase chars)
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("testclass", newCandidateName);

    candidateName = "TESTCLASS";    //  NOT acceptable (no lowercase chars)
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("TESTCLASS", newCandidateName);

    // White space

    candidateName = "test Class";    //  NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("test_Class", newCandidateName);

    candidateName = "Test class";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("Test_class", newCandidateName);

    candidateName = "Test Class";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("Test_Class", newCandidateName);

    candidateName = "test class";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("test_class", newCandidateName);

    candidateName = "TEST CLASS";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("TEST_CLASS", newCandidateName);

    // Underscore

    candidateName = "test_Class";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("test_Class", newCandidateName);

    candidateName = "Test_class";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("Test_class", newCandidateName);

    candidateName = "Test_Class";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("Test_Class", newCandidateName);

    candidateName = "test_class";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("test_class", newCandidateName);

    candidateName = "TEST_CLASS";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexName(candidateName);
    assertEquals("TEST_CLASS", newCandidateName);

  }

  @Test

  /*
   * Resolve VertexProperty (Original Variable Convention)
   */

  public void test2() {

    String candidateName = "";
    String newCandidateName = "";

    // No white space nor underscore

    candidateName = "testVariable";    // acceptable (one or more uppercase char, except the first one)
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("testVariable", newCandidateName);

    candidateName = "Testvariable";    // NOT acceptable (one or more uppercase char, the first one included)
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("Testvariable", newCandidateName);

    candidateName = "TestVariable";    // NOT acceptable (one or more uppercase char, except the first one)
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("TestVariable", newCandidateName);

    candidateName = "testvariable";    // acceptable (no uppercase chars)
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("testvariable", newCandidateName);

    candidateName = "TESTVARIABLE";    // NOT acceptable (no lowercase chars)
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("TESTVARIABLE", newCandidateName);

    // White space

    candidateName = "test Variable";    //  NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("test_Variable", newCandidateName);

    candidateName = "Test variable";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("Test_variable", newCandidateName);

    candidateName = "Test Variable";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("Test_Variable", newCandidateName);

    candidateName = "test variable";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("test_variable", newCandidateName);

    candidateName = "TEST VARIABLE";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("TEST_VARIABLE", newCandidateName);

    // Underscore

    candidateName = "test_Variable";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("test_Variable", newCandidateName);

    candidateName = "Test_variable";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("Test_variable", newCandidateName);

    candidateName = "Test_Variable";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("Test_Variable", newCandidateName);

    candidateName = "test_variable";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("test_variable", newCandidateName);

    candidateName = "TEST_VARIABLE";    // NOT acceptable
    newCandidateName = nameResolver.resolveVertexProperty(candidateName);
    assertEquals("TEST_VARIABLE", newCandidateName);

  }
}
