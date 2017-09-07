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

  private int                  level;    // affects OutputStreamManager level
  private OOutputStreamManager outputManager;

  public OTeleporterMessageHandler(PrintStream outputStream, int level) {
    this.level = level;
    this.outputManager = new OOutputStreamManager(outputStream, level);
  }

  public OTeleporterMessageHandler(int level) {
    this.level = level;
    this.outputManager = new OOutputStreamManager(level);
  }

  public OTeleporterMessageHandler(OOutputStreamManager outputStreamManager) {
    this.outputManager = outputStreamManager;
    this.level = this.outputManager.getLevel();
  }

  public OOutputStreamManager getOutputManager() {
    return this.outputManager;
  }

  public void setOutputManager(OOutputStreamManager outputManager) {
    this.outputManager = outputManager;
  }


  @Override
  public int getLevel() {
    return this.level;
  }

  @Override
  public void setLevel(int level) {
    this.level = level;
    this.updateOutputStreamManagerLevel();
  }

  private synchronized void updateOutputStreamManagerLevel() {
    this.outputManager.setLevel(this.level);
  }

  @Override
  public synchronized void debug(String message) {
    this.outputManager.debug(message);
  }

  @Override
  public synchronized void debug(String format, Object... args) {
    this.outputManager.debug(format, args);
  }

  @Override
  public synchronized void info(String message) {
    this.outputManager.info(message);
  }

  @Override
  public synchronized void info(String format, Object... args) {
    this.outputManager.info(format, args);
  }

  @Override
  public synchronized void warn(String message) {
    this.outputManager.warn(message);
  }

  @Override
  public synchronized void warn(String format, Object... args) {
    this.outputManager.warn(format, args);
  }

  @Override
  public synchronized void error(String message) {
    this.outputManager.error(message);
  }

  @Override
  public synchronized void error(String format, Object... args) {
    this.outputManager.error(format, args);
  }
}
