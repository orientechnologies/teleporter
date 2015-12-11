/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *   
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *   
 *        http://www.apache.org/licenses/LICENSE-2.0
 *   
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *   
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.teleporter.http.handler;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.main.OTeleporter;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.UUID;

/**
 * Created by Enrico Risa on 27/11/15.
 */
public class OTeleporterJob implements Runnable {

  private final ODocument     cfg;
  private OTeleporterListener listener;

  public String               id;

  public Status               status;
  public PrintStream          stream;
  ByteArrayOutputStream       baos;

  public OTeleporterJob(ODocument cfg, OTeleporterListener listener) {
    this.cfg = cfg;
    this.listener = listener;

    baos = new ByteArrayOutputStream();
    stream = new PrintStream(baos);
  }

  @Override
  public void run() {

    id = UUID.randomUUID().toString();

    final String driver = cfg.field("driver");
    final String jurl = cfg.field("jurl");
    final String username = cfg.field("username");
    final String password = cfg.field("password");
    final String outDbUrl = cfg.field("outDbUrl");
    final String chosenStrategy = cfg.field("strategy");
    final String chosenMapper = cfg.field("mapper");
    final String xmlPath = cfg.field("xmlPath");
    final String nameResolver = cfg.field("nameResolver");
    final String outputLevel = cfg.field("level");
    final List<String> includedTables = cfg.field("includes");
    final List<String> excludedTable = cfg.field("excludes");
    status = Status.RUNNING;
    try {
      OTeleporter.execute(driver, jurl, username, password, outDbUrl, chosenStrategy, chosenMapper, xmlPath, nameResolver,
          outputLevel, includedTables, excludedTable, new OOutputStreamManager(stream, 2));
    } catch (Exception e) {
    }

    synchronized (listener) {
      status = Status.FINISHED;
      try {
        listener.wait(5000);
        listener.onEnd(this);
      } catch (InterruptedException e) {
      }
    }

  }

  public void validate() {

  }

  /**
   * Single Job Status
   * 
   * @return ODocument
   */
  public ODocument status() {

    synchronized (listener) {
      ODocument status = new ODocument();
      status.field("cfg", cfg);
      status.field("status", this.status);
      status.field("log", baos.toString());
      if (this.status == Status.FINISHED) {
        listener.notifyAll();
      }
      return status;
    }

  }

  public enum Status {
    STARTED, RUNNING, FINISHED
  }
}
