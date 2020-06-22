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

package com.orientechnologies.teleporter.http.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.output.OPluginMessageHandler;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.teleporter.context.OTeleporterMessageHandler;
import com.orientechnologies.teleporter.exception.OTeleporterIOException;
import com.orientechnologies.teleporter.exception.OTeleporterRuntimeException;
import com.orientechnologies.teleporter.main.OTeleporter;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/** Created by Enrico Risa on 27/11/15. */
public class OTeleporterJob implements Callable<ODocument> {

  private final ODocument cfg;
  private OTeleporterListener listener;

  private String id;

  private Status status;
  private PrintStream stream;
  private ByteArrayOutputStream baos;
  private OPluginMessageHandler messageHandler;

  private OServer currentServerInstance;

  public OTeleporterJob(
      ODocument cfg, OServer currentServerInstance, OTeleporterListener listener) {
    this.cfg = cfg;
    this.listener = listener;

    this.baos = new ByteArrayOutputStream();
    this.stream = new PrintStream(baos);

    this.currentServerInstance = currentServerInstance;
  }

  @Override
  public ODocument call() {

    id = UUID.randomUUID().toString();

    String serverDatabaseDirectory = this.currentServerInstance.getDatabaseDirectory();

    final String driver = cfg.field("driver");
    final String jurl = cfg.field("jurl");
    final String username = cfg.field("username");
    final String password = cfg.field("password");
    final String protocol = cfg.field("protocol");
    final String outDbName = cfg.field("outDBName");
    final String chosenStrategy = cfg.field("strategy");
    final String chosenMapper = cfg.field("mapper");
    final String xmlPath = cfg.field("xmlPath");
    final String nameResolver = cfg.field("nameResolver");
    final String outputLevel = cfg.field("level");
    final List<String> includedTables = cfg.field("includedTables");
    final List<String> excludedTable = null;
    final String migrationConfig = cfg.field("migrationConfig");
    status = Status.RUNNING;

    OrientDB orientDBInstance = currentServerInstance.getContext();
    int msgHandlerLevel = Integer.parseInt(outputLevel);
    this.messageHandler = new OTeleporterMessageHandler(this.stream, msgHandlerLevel);

    String outDbUrl;
    if (protocol.equals("plocal")) {
      outDbUrl = protocol + ":" + serverDatabaseDirectory + outDbName;
    } else {
      // protocol.equals("memory")
      outDbUrl = protocol + ":" + outDbName;
    }

    ODocument executionResult = null;
    try {
      if (chosenStrategy.equals("interactive") || chosenStrategy.equals("interactive-aggr")) {
        executionResult =
            OTeleporter.executeJob(
                driver,
                jurl,
                username,
                password,
                outDbUrl,
                chosenStrategy,
                chosenMapper,
                xmlPath,
                nameResolver,
                outputLevel,
                includedTables,
                excludedTable,
                migrationConfig,
                this.messageHandler,
                orientDBInstance);

        synchronized (listener) {
          status = Status.FINISHED;
          try {
            listener.wait(5000);
            listener.onEnd(this);
          } catch (InterruptedException e) {
          }
        }
      } else {
        new Thread(
                new Runnable() {
                  @Override
                  public void run() {
                    try {
                      OTeleporter.executeJob(
                          driver,
                          jurl,
                          username,
                          password,
                          outDbUrl,
                          chosenStrategy,
                          chosenMapper,
                          xmlPath,
                          nameResolver,
                          outputLevel,
                          includedTables,
                          excludedTable,
                          migrationConfig,
                          new OTeleporterMessageHandler(stream, 2),
                          orientDBInstance);
                    } catch (OTeleporterIOException e) {
                      e.printStackTrace();
                    }
                    synchronized (listener) {
                      status = Status.FINISHED;
                      try {
                        listener.wait(5000);
                        listener.onEnd(OTeleporterJob.this);
                      } catch (InterruptedException e) {
                      }
                    }
                  }
                })
            .start();
        executionResult = new ODocument();
      }
    } catch (Exception e) {
      throw new OTeleporterRuntimeException(e.getMessage());
    }

    return executionResult;
  }

  public void validate() {}

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

      String lastBatchLog = extractBatchLog();
      status.field("log", lastBatchLog);

      if (this.status == Status.FINISHED) {
        listener.notifyAll();
      }
      return status;
    }
  }

  private String extractBatchLog() {

    String lastBatchLog = "Current status not correctly loaded.";

    synchronized (this.messageHandler) {

      // filling the last log batch
      int baosInitSize = baos.size();
      try {
        lastBatchLog = baos.toString("UTF-8");
      } catch (Exception e) {
        e.printStackTrace();
      }
      int baosFinalSize = baos.size();
      if (baosFinalSize - baosInitSize > 0) {
        OLogManager.instance().info(this, "[Teleporter] Losing some buffer info.");
      } else {
        baos.reset();
      }
    }
    return lastBatchLog;
  }

  public enum Status {
    STARTED,
    RUNNING,
    FINISHED
  }
}
