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

package com.orientechnologies.teleporter.http;

import com.orientechnologies.teleporter.http.handler.OTeleporterHandler;
import com.orientechnologies.teleporter.util.ODriverConfigurator;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.IOException;

/**
 * Created by Enrico Risa on 26/11/15.
 */
public class OServerCommandTeleporter extends OServerCommandAuthenticatedServerAbstract {

  OTeleporterHandler handler = new OTeleporterHandler();
  private static final String[] NAMES = { "GET|teleporter/*", "POST|teleporter/*" };

  public OServerCommandTeleporter() {
    super("server.profiler");
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: auditing/<db>/<action>");

    if ("POST".equalsIgnoreCase(iRequest.httpMethod)) {
      doPost(iRequest, iResponse, parts);
    }
    if ("GET".equalsIgnoreCase(iRequest.httpMethod)) {
      doGet(iRequest, iResponse, parts);
    }
    return false;
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts) throws IOException {

    if ("status".equalsIgnoreCase(parts[1])) {
      ODocument status = handler.status();
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, status.toJSON("prettyPrint"), null);

    } else if ("drivers".equalsIgnoreCase(parts[1])) {

      ODriverConfigurator configurator = new ODriverConfigurator();

      ODocument drivers = configurator.readJsonFromUrl(ODriverConfigurator.DRIVERS);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, drivers.toJSON("prettyPrint"), null);
    } else {
      throw new IllegalArgumentException("");
    }
  }

  private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts) throws IOException {

    if ("job".equalsIgnoreCase(parts[1])) {
      ODocument args = new ODocument().fromJSON(iRequest.content);
      ODocument executionResult = handler.execute(args, super.server);

      if (executionResult != null) {
        // the result corresponds to the graph model representation
        String jsonGraphModel = ((ODocument) executionResult).toJSON("prettyPrint");
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, jsonGraphModel, null);
      } else {
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);
      }

    } else if ("test".equalsIgnoreCase(parts[1])) {
      ODocument args = new ODocument().fromJSON(iRequest.content);
      try {
        handler.checkConnection(args, super.server);
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);

    } else if ("tables".equalsIgnoreCase(parts[1])) {
      ODocument params = new ODocument().fromJSON(iRequest.content);
      ODocument tables;
      try {
        tables = handler.getTables(params, super.server);
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, tables.toJSON("prettyPrint"), null);
    } else {
      throw new IllegalArgumentException("");
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}