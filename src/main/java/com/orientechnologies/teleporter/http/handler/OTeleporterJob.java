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

package com.orientechnologies.teleporter.http.handler;

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.exception.OTeleporterIOException;
import com.orientechnologies.teleporter.main.OTeleporter;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Created by Enrico Risa on 27/11/15.
 */
public class OTeleporterJob implements Callable<ODocument> {

  private final ODocument           cfg;
  private       OTeleporterListener listener;

  public String id;

  public Status      status;
  public PrintStream stream;
  ByteArrayOutputStream baos;

  public OTeleporterJob(ODocument cfg, OTeleporterListener listener) {
    this.cfg = cfg;
    this.listener = listener;

    baos = new ByteArrayOutputStream();
    stream = new PrintStream(baos);
  }

  @Override
  public ODocument call() {

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
    final List<String> includedTables = cfg.field("includedTables");
    final List<String> excludedTable = null;
    final String migrationConfig = cfg.field("migrationConfig");
    status = Status.RUNNING;

    ODocument executionResult = null;
    try {
      if (chosenStrategy.equals("interactive") || chosenStrategy.equals("interactive-aggr")) {
        executionResult = OTeleporter
            .executeJob(driver, jurl, username, password, outDbUrl, chosenStrategy, chosenMapper, xmlPath, nameResolver,
                outputLevel, includedTables, excludedTable, migrationConfig, new OOutputStreamManager(stream, 2));

        synchronized (listener) {
          status = Status.FINISHED;
          try {
            listener.wait(5000);
            listener.onEnd(this);
          } catch (InterruptedException e) {
          }
        }
      } else {
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              OTeleporter
                  .executeJob(driver, jurl, username, password, outDbUrl, chosenStrategy, chosenMapper, xmlPath, nameResolver,
                      outputLevel, includedTables, excludedTable, migrationConfig, new OOutputStreamManager(stream, 2));
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
        }).start();
        executionResult = new ODocument();
      }
    } catch (Exception e) {
    }

    return executionResult;
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