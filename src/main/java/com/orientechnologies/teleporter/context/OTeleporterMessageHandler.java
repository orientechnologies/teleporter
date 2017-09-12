package com.orientechnologies.teleporter.context;

import com.orientechnologies.orient.output.OOutputStreamManager;
import com.orientechnologies.orient.output.OPluginMessageHandler;

import java.io.PrintStream;

/**
 * Implementation of OPluginMessageHandler for Teleporter plugin.
 * It receives messages application from the application and just delegates its printing on a stream through the OutputStreamManager.
 *
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */
public class OTeleporterMessageHandler implements OPluginMessageHandler {

  private int                  outputManagerLevel;    // affects OutputStreamManager level
  private OOutputStreamManager outputManager;

  public OTeleporterMessageHandler(PrintStream outputStream, int level) {
    this.outputManagerLevel = level;
    this.outputManager = new OOutputStreamManager(outputStream, level);
  }

  public OTeleporterMessageHandler(int level) {
    this.outputManagerLevel = level;
    this.outputManager = new OOutputStreamManager(level);
  }

  public OTeleporterMessageHandler(OOutputStreamManager outputStreamManager) {
    this.outputManager = outputStreamManager;
    this.outputManagerLevel = this.outputManager.getLevel();
  }

  public OOutputStreamManager getOutputManager() {
    return this.outputManager;
  }

  public void setOutputManager(OOutputStreamManager outputManager) {
    this.outputManager = outputManager;
  }


  @Override
  public int getOutputManagerLevel() {
    return this.outputManagerLevel;
  }

  @Override
  public void setOutputManagerLevel(int level) {
    this.outputManagerLevel = level;
    this.updateOutputStreamManagerLevel();
  }

  private synchronized void updateOutputStreamManagerLevel() {
    this.outputManager.setLevel(this.outputManagerLevel);
  }

  @Override
  public synchronized void debug(Object requester, String message) {
    this.outputManager.debug(message);
  }

  @Override
  public synchronized void debug(Object requester, String format, Object... args) {
    this.outputManager.debug(format, args);
  }

  @Override
  public synchronized void info(Object requester, String message) {
    this.outputManager.info(message);
  }

  @Override
  public synchronized void info(Object requester, String format, Object... args) {
    this.outputManager.info(format, args);
  }

  @Override
  public synchronized void warn(Object requester, String message) {
    this.outputManager.warn(message);
  }

  @Override
  public synchronized void warn(Object requester, String format, Object... args) {
    this.outputManager.warn(format, args);
  }

  @Override
  public synchronized void error(Object requester, String message) {
    this.outputManager.error(message);
  }

  @Override
  public synchronized void error(Object requester, String format, Object... args) {
    this.outputManager.error(format, args);
  }
}
