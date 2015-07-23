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

package com.orientechnologies.orient.teleport.context;

import java.io.OutputStream;
import java.io.PrintStream;

/**
 * It contains and manages an OutputStream at different and desired levels.
 * The default OutputStream is 'System.out', but it's possible to instantiate the class with
 * a specific one by passing it to the class Constructor.
 * Levels:
 * - 0 : no output
 * - 3 : only error level is printed
 * - 2 : from info to error is printed
 * - 1 : from debug to error is printed
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class OOutputStreamManager {

  public PrintStream outputStream;
  private int level;
  private final int BLANK_LEVEL = 0;
  private final int DEBUG_LEVEL = 1;
  private final int INFO_LEVEL = 2;
  private final int WARNING_LEVEL = 3;
  private final int ERROR_LEVEL = 4;


  public OOutputStreamManager(int level) {
    this.outputStream = System.out;
    this.level = level;
  }

  public OOutputStreamManager(PrintStream outputStream, int level) {
    this.outputStream = outputStream;
    this.level = level;
  }


  public OutputStream getOutputStream() {
    return outputStream;
  }
  
  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public void debug(String message) {
    if(!(this.level == BLANK_LEVEL)) {
      if(this.level <= DEBUG_LEVEL ) 
        this.outputStream.println(message);
    }
  }
  
  public void debug(String format, Object... args) {
    if(!(this.level == BLANK_LEVEL)) {
      if(this.level <= DEBUG_LEVEL ) 
        this.outputStream.printf(format, args);
    }
  }

  public void info(String message) {
    if(!(this.level == BLANK_LEVEL)) {
      if(this.level <= INFO_LEVEL)
        this.outputStream.println(message);
    }
  }
  
  public void info(String format,  Object... args) {
    if(!(this.level == BLANK_LEVEL)) {
      if(this.level <= INFO_LEVEL)
        this.outputStream.printf(format, args);
    }
  }

  public void warn(String message) {
    if(!(this.level == BLANK_LEVEL)) {
      if(this.level <= WARNING_LEVEL)
        this.outputStream.println(message);
    }
  }
  
  public void warn(String format, Object... args) {
    if(!(this.level == BLANK_LEVEL)) {
      if(this.level <= WARNING_LEVEL)
        this.outputStream.printf(format, args);
    }
  }

  public void error(String message) {
    if(!(this.level == BLANK_LEVEL)) {
      if(this.level <= ERROR_LEVEL)
        this.outputStream.println("ERROR: " + message);
    }
  }
  
  public void error(String format, Object... args) {
    if(!(this.level == BLANK_LEVEL)) {
      if(this.level <= ERROR_LEVEL)
        this.outputStream.printf(format, "ERROR: ", args);
    }
  }
  






}
