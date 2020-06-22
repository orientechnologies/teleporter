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

package com.orientechnologies.teleporter.test.rdbms.util;

import static org.junit.Assert.*;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.util.OFileManager;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.junit.Test;

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

      OFileManager.extractAll(
          "src/test/resources/file-manager/sample.zip", "src/test/resources/file-manager/");
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

      OFileManager.extractAll(
          "src/test/resources/file-manager/sample.zip", "src/test/resources/file-manager/");
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

      ODocument document =
          OFileManager.buildJsonFromFile("src/test/resources/file-manager/sample.json");
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
