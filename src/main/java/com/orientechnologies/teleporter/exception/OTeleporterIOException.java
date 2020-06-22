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

package com.orientechnologies.teleporter.exception;

import java.io.IOException;

/**
 * It represents an IO Exception in Teleporter.
 *
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
@SuppressWarnings("serial")
public class OTeleporterIOException extends IOException {

  public OTeleporterIOException() {}

  public OTeleporterIOException(String message) {
    super(message);
  }

  public OTeleporterIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public OTeleporterIOException(Throwable cause) {
    super(cause);
  }
}
