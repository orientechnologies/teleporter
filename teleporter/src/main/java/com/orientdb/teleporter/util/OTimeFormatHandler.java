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

package com.orientdb.teleporter.util;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Static class which manages time's format offering different methods.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class OTimeFormatHandler {


  public static String getHMSFormat(Date start, Date end) {

    String hmsTime = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime()), TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())
        - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(end.getTime()-start.getTime())), TimeUnit.MILLISECONDS.toSeconds(end.getTime()-start.getTime())
        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(end.getTime()-start.getTime())));

    return hmsTime;
  }

  public static String getHMSFormat(long millis) {

    String hmsTime = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis), TimeUnit.MILLISECONDS.toMinutes(millis)
        - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)), TimeUnit.MILLISECONDS.toSeconds(millis)
        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));

    return hmsTime;
  }


}
