/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.teleporter.util;

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
