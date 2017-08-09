/*
 * Copyright 2016 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.teleporter.test.rdbms.util;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.util.OFileManager;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class FileManagerTest {

  @Test
  public void deleteFileTest() {

    try {
      File newFile = new File("src/test/resources/sample/empty-file");
      newFile.getParentFile().mkdirs();
      newFile.createNewFile();

      File dir = new File("src/test/resources/sample/");

      assertTrue(dir.exists());
      assertTrue(newFile.exists());

      OFileManager.deleteResource(dir.getPath());

      assertFalse(dir.exists());
      assertFalse(newFile.exists());

    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

  }

  @Test
  public void extractAllTest() {

    try {

      OFileManager.extractAll("src/test/resources/file-manager/sample.zip", "src/test/resources/file-manager/");
      File dir = new File("src/test/resources/file-manager/sample/");
      File file = new File("src/test/resources/file-manager/sample/empty-file");

      assertTrue(dir.exists());
      assertTrue(file.exists());

      OFileManager.deleteResource(dir.getPath());

    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

  }

  @Test
  public void readAllTest() {

    try {

      OFileManager.extractAll("src/test/resources/file-manager/sample.zip", "src/test/resources/file-manager/");
      File dir = new File("src/test/resources/file-manager/sample/");
      File file = new File("src/test/resources/file-manager/sample/empty-file");

      FileReader fileReader = new FileReader(file);
      String fileContent = OFileManager.readAllTextFile(fileReader);

      assertEquals("Hello, this is a sample file!\n", fileContent);

      fileReader.close();
      OFileManager.deleteResource(dir.getPath());

    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void buildJsonFromFileTest() {

    try {

      ODocument document = OFileManager.buildJsonFromFile("src/test/resources/file-manager/sample.json");
      assertNotNull(document);

      ODocument person = document.field("Person");
      assertNotNull(person);

      String firstName = person.field("firstName");
      String lastName = person.field("lastName");
      String age = person.field("age");

      assertNotNull(firstName);
      assertNotNull(lastName);
      assertNotNull(age);

      assertEquals("Peter", firstName);
      assertEquals("Brown", lastName);
      assertEquals("55", age);

      ODocument document2 = null;
      try {
        document2 = OFileManager.buildJsonFromFile("src/test/resources/file-manager/sample2.json");
      } catch (Exception e) {
      }
      assertNull(document2);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
