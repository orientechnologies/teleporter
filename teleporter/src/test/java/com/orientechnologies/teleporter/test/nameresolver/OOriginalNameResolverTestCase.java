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

package com.orientechnologies.teleporter.test;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.teleporter.nameresolver.OOriginalConventionNameResolver;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OOriginalNameResolverTestCase {
	
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
		
		candidateName = "testClass";		// NOT acceptable (one or more uppercase char, except the first one)
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("testClass", newCandidateName);
		
		candidateName = "Testclass";		// acceptable (one or more uppercase char, the first one included)
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("Testclass", newCandidateName);

		candidateName = "TestClass";		// acceptable (one or more uppercase char, except the first one)
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("TestClass", newCandidateName);

		candidateName = "testclass";		// NOT acceptable (no uppercase chars)
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("testclass", newCandidateName);

		candidateName = "TESTCLASS";		//  NOT acceptable (no lowercase chars)
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("TESTCLASS", newCandidateName);

		// White space

		candidateName = "test Class";		//  NOT acceptable
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("test_Class", newCandidateName);

		candidateName = "Test class";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("Test_class", newCandidateName);

		candidateName = "Test Class";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("Test_Class", newCandidateName);

		candidateName = "test class";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("test_class", newCandidateName);

		candidateName = "TEST CLASS";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("TEST_CLASS", newCandidateName);
		

		// Underscore

		candidateName = "test_Class";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("test_Class", newCandidateName);

		candidateName = "Test_class";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("Test_class", newCandidateName);

		candidateName = "Test_Class";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("Test_Class", newCandidateName);

		candidateName = "test_class";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexName(candidateName);
		assertEquals("test_class", newCandidateName);

		candidateName = "TEST_CLASS";		// NOT acceptable
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
		
		candidateName = "testVariable";		// acceptable (one or more uppercase char, except the first one)
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("testVariable", newCandidateName);
		
		candidateName = "Testvariable";		// NOT acceptable (one or more uppercase char, the first one included)
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("Testvariable", newCandidateName);

		candidateName = "TestVariable";		// NOT acceptable (one or more uppercase char, except the first one)
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("TestVariable", newCandidateName);

		candidateName = "testvariable";		// acceptable (no uppercase chars)
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("testvariable", newCandidateName);

		candidateName = "TESTVARIABLE";		// NOT acceptable (no lowercase chars)
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("TESTVARIABLE", newCandidateName);

		// White space

		candidateName = "test Variable";		//  NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("test_Variable", newCandidateName);

		candidateName = "Test variable";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("Test_variable", newCandidateName);

		candidateName = "Test Variable";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("Test_Variable", newCandidateName);

		candidateName = "test variable";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("test_variable", newCandidateName);

		candidateName = "TEST VARIABLE";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("TEST_VARIABLE", newCandidateName);
		

		// Underscore

		candidateName = "test_Variable";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("test_Variable", newCandidateName);

		candidateName = "Test_variable";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("Test_variable", newCandidateName);

		candidateName = "Test_Variable";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("Test_Variable", newCandidateName);

		candidateName = "test_variable";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("test_variable", newCandidateName);

		candidateName = "TEST_VARIABLE";		// NOT acceptable
		newCandidateName = nameResolver.resolveVertexProperty(candidateName);
		assertEquals("TEST_VARIABLE", newCandidateName);

	}

}
