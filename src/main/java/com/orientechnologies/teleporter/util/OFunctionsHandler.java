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

package com.orientechnologies.teleporter.util;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Static class which manages time's format offering different methods.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OFunctionsHandler {

  public static String getHMSFormat(Date start, Date end) {

    String hmsTime =
        String.format(
            "%02d:%02d:%02d",
            TimeUnit.MILLISECONDS.toHours(end.getTime() - start.getTime()),
            TimeUnit.MILLISECONDS.toMinutes(end.getTime() - start.getTime())
                - TimeUnit.HOURS.toMinutes(
                    TimeUnit.MILLISECONDS.toHours(end.getTime() - start.getTime())),
            TimeUnit.MILLISECONDS.toSeconds(end.getTime() - start.getTime())
                - TimeUnit.MINUTES.toSeconds(
                    TimeUnit.MILLISECONDS.toMinutes(end.getTime() - start.getTime())));

    return hmsTime;
  }

  public static String getHMSFormat(long millis) {

    String hmsTime =
        String.format(
            "%02d:%02d:%02d",
            TimeUnit.MILLISECONDS.toHours(millis),
            TimeUnit.MILLISECONDS.toMinutes(millis)
                - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
            TimeUnit.MILLISECONDS.toSeconds(millis)
                - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));

    return hmsTime;
  }

  public static boolean haveDocumentsSameContent(ODocument d1, ODocument d2) {

    /*
     * properties check
     * (i) number
     * (ii) name
     * (iii) content (recursive)
     */

    // (i) number of properties
    if (d1.toMap().keySet().size() != d2.toMap().keySet().size()) {
      return false;
    }

    for (String key1 : d1.toMap().keySet()) {

      // (ii) name of fields
      if (!d2.toMap().keySet().contains(key1)) {
        return false;
      } else {

        // (iii) content (recursive)
        Object obj1 = d1.toMap().get(key1);
        Object obj2 = d2.toMap().get(key1);

        if (obj1 instanceof String && obj2 instanceof String) { // base case: value is a string
          if (!obj1.equals(obj2)) {
            return false;
          }
        } else if (obj1 instanceof ODocument
            && obj2
                instanceof
                ODocument) { // inductive case: two values are documents, recursive comparison

          if (!haveDocumentsSameContent(((ODocument) obj1), ((ODocument) obj2))) {
            return false;
          }
        } else {
          return false; // different aggregation level --> values not equivalent
        }
      }
    }

    return true;
  }
}
