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

import com.orientechnologies.teleporter.util.OTimeFormatHandler;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 *
 */

public class TimeFormatHandlerTest {

  @Test
  public void test() {

    Date start = new Date();
    long endMillis = start.getTime() + 7713000L;
    Date end = new Date(endMillis);

    String timeFormat1 = OTimeFormatHandler.getHMSFormat(start, end);
    assertEquals("02:08:33", timeFormat1);

    String timeFormat2 = OTimeFormatHandler.getHMSFormat(7713000L);
    assertEquals("02:08:33", timeFormat2);

  }

}
