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
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.util.ODriverConfigurator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Enrico Risa on 27/11/15.
 */
public class OTeleporterHandler {
	private ExecutorService pool       = Executors.newFixedThreadPool(1);

	OTeleporterJob          currentJob = null;

	/**
	 * Execute import with configuration;
	 * 
	 * @param cfg
	 */
	public void executeImport(ODocument cfg) {

		OTeleporterJob job = new OTeleporterJob(cfg, new OTeleporterListener() {
			@Override
			public void onEnd(OTeleporterJob oTeleporterJob) {
				currentJob = null;
			}
		});

		job.validate();

		currentJob = job;
		pool.execute(job);

	}

	/**
	 * Check If the connection with given parameters is alive
	 * 
	 * @param cfg
	 * @throws Exception
	 */
	public void checkConnection(ODocument cfg) throws Exception {

		ODriverConfigurator configurator = new ODriverConfigurator();

		final String driver = cfg.field("driver");
		final String jurl = cfg.field("jurl");
		final String username = cfg.field("username");
		final String password = cfg.field("password");
		OTeleporterContext oTeleporterContext = new OTeleporterContext();
		oTeleporterContext.setOutputManager(new OOutputStreamManager(2));
		configurator.checkConnection(driver, jurl, username, password, oTeleporterContext);
	}

	/**
	 * Status of the Running Jobs
	 * 
	 * @return ODocument
	 */
	public ODocument status() {

		ODocument status = new ODocument();

		Collection<ODocument> jobs = new ArrayList<ODocument>();
		if (currentJob != null) {
			jobs.add(currentJob.status());
		}
		status.field("jobs", jobs);
		return status;
	}
}
