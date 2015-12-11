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

package com.orientechnologies.teleporter.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import com.orientechnologies.teleporter.http.handler.OTeleporterHandler;
import com.orientechnologies.teleporter.util.ODriverConfigurator;

import java.io.IOException;

/**
 * Created by Enrico Risa on 26/11/15.
 */
public class OServerCommandTeleporter extends OServerCommandAuthenticatedServerAbstract {

  OTeleporterHandler            handler = new OTeleporterHandler();
  private static final String[] NAMES   = { "GET|teleporter/*", "POST|teleporter/*" };

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

      ODocument drivers = configurator.readJsonFromUrl(ODriverConfigurator.DRIVERS, null);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, drivers.toJSON("prettyPrint"), null);
    } else {
      throw new IllegalArgumentException("");
    }
  }

  private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts) throws IOException {

    if ("job".equalsIgnoreCase(parts[1])) {
      ODocument cfg = new ODocument().fromJSON(iRequest.content);
      handler.executeImport(cfg);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);

    } else if ("test".equalsIgnoreCase(parts[1])) {
      ODocument cfg = new ODocument().fromJSON(iRequest.content);
      try {
        handler.checkConnection(cfg);
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);

    } else {
      throw new IllegalArgumentException("");
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
