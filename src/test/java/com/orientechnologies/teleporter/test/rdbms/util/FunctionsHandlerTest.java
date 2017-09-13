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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.util.OFunctionsHandler;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */

public class FunctionsHandlerTest {

  @Test
  public void timeFormatsTest() {

    Date start = new Date();
    long endMillis = start.getTime() + 7713000L;
    Date end = new Date(endMillis);

    String timeFormat1 = OFunctionsHandler.getHMSFormat(start, end);
    assertEquals("02:08:33", timeFormat1);

    String timeFormat2 = OFunctionsHandler.getHMSFormat(7713000L);
    assertEquals("02:08:33", timeFormat2);

  }

  @Test
  public void documentEqualsTest() {

    // two identical documents

    String stringDoc1 = "{\n" + "\t\"name\": \"Book the First\",\n" + "\t\"author\": {\n" + "\t\t\"first_name\": \"Bob\",\n"
        + "\t\t\"last_name\": \"White\",\n" + "\t\t\"age\": \"45\",\n" + "\t\t\"address\": {\n"
        + "\t\t\t\"street\": \"Foo Street 123\",\n" + "\t\t\t\"zip_code\": \"90001\",\n" + "\t\t\t\"city\": \"Los Angeles\"\n"
        + "\t\t}\n" + "\t}\n" + "}";

    String stringDoc2 = "{\n" + "\t\"name\": \"Book the First\",\n" + "\t\"author\": {\n" + "\t\t\"first_name\": \"Bob\",\n"
        + "\t\t\"last_name\": \"White\",\n" + "\t\t\"age\": \"45\",\n" + "\t\t\"address\": {\n"
        + "\t\t\t\"street\": \"Foo Street 123\",\n" + "\t\t\t\"zip_code\": \"90001\",\n" + "\t\t\t\"city\": \"Los Angeles\"\n"
        + "\t\t}\n" + "\t}\n" + "}";

    ODocument document1 = new ODocument();
    document1.fromJSON(stringDoc1, "noMap");

    ODocument document2 = new ODocument();
    document2.fromJSON(stringDoc2, "noMap");

    assertTrue(OFunctionsHandler.haveDocumentsSameContent(document1, document2) == true);

    // two documents with the same content (fields' order not equal)

    stringDoc2 = "{\n" + "\t\"name\": \"Book the First\",\n" + "\t\"author\": {\n" + "\t\t\"age\": \"45\",\n"
        + "\t\t\"first_name\": \"Bob\",\n" + "\t\t\"last_name\": \"White\",\n" + "\t\t\"address\": {\n"
        + "\t\t\t\"city\": \"Los Angeles\",\n" + "\t\t\t\"zip_code\": \"90001\",\n" + "\t\t\t\"street\": \"Foo Street 123\"\n"
        + "\t\t}\n" + "\t}\n" + "}";

    document2 = new ODocument();
    document2.fromJSON(stringDoc2, "noMap");

    assertTrue(OFunctionsHandler.haveDocumentsSameContent(document1, document2) == true);

    // two documents with different content (different values)

    stringDoc2 = "{\n" + "\t\"name\": \"Book the First\",\n" + "\t\"author\": {\n" + "\t\t\"age\": \"45\",\n"
        + "\t\t\"first_name\": \"Bob\",\n" + "\t\t\"last_name\": \"White\",\n" + "\t\t\"address\": {\n"
        + "\t\t\t\"city\": \"Los Angeles\",\n" + "\t\t\t\"zip_code\": \"90002\",\n" + "\t\t\t\"street\": \"Foo Street 456\"\n"
        + "\t\t}\n" + "\t}\n" + "}";

    document2 = new ODocument();
    document2.fromJSON(stringDoc2, "noMap");

    assertTrue(OFunctionsHandler.haveDocumentsSameContent(document1, document2) == false);

    // two documents with different content (different number of fields)

    stringDoc2 = "{\n" + "\t\"name\": \"Book the First\",\n" + "\t\"author\": {\n" + "\t\t\"first_name\": \"Bob\",\n"
        + "\t\t\"last_name\": \"White\",\n" + "\t\t\"address\": {\n" + "\t\t\t\"city\": \"Los Angeles\",\n"
        + "\t\t\t\"street\": \"Foo Street 456, 90002\"\n" + "\t\t}\n" + "\t}\n" + "}";

    document2 = new ODocument();
    document2.fromJSON(stringDoc2, "noMap");

    assertTrue(OFunctionsHandler.haveDocumentsSameContent(document1, document2) == false);

    // two documents with different content (different fields' name)

    stringDoc2 = "{\n" + "\t\"name\": \"Book the First\",\n" + "\t\"author\": {\n" + "\t\t\"name\": \"Bob\",\n"
        + "\t\t\"surname\": \"White\",\n" + "\t\t\"age\": \"45\",\n" + "\t\t\"address\": {\n"
        + "\t\t\t\"street\": \"Foo Street 123\",\n" + "\t\t\t\"zip_code\": \"90001\",\n" + "\t\t\t\"city\": \"Los Angeles\"\n"
        + "\t\t}\n" + "\t}\n" + "}";

    document2 = new ODocument();
    document2.fromJSON(stringDoc2, "noMap");

    assertTrue(OFunctionsHandler.haveDocumentsSameContent(document1, document2) == false);

    // two documents with different content (at least one field in doc1 is a nested document while the correspondent field in doc2 is a string)

    stringDoc2 = "{\n" + "\t\"name\": \"Book the First\",\n" + "\t\"author\": {\n" + "\t\t\"age\": \"45\",\n"
        + "\t\t\"first_name\": \"Bob\",\n" + "\t\t\"last_name\": \"White\",\n"
        + "\t\t\"address\": \"Foo Street 123, 90001, Los Angeles\"\n" + "\t}\n" + "}";

    document2 = new ODocument();
    document2.fromJSON(stringDoc2, "noMap");

    assertTrue(OFunctionsHandler.haveDocumentsSameContent(document1, document2) == false);

  }

}
