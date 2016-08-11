/*
 * Copyright 2016 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.teleporter.test.rdbms.configuration.importing;

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.persistence.handler.OHSQLDBDataTypeHandler;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveAggregationStrategy;
import com.orientechnologies.teleporter.strategy.rdbms.ODBMSNaiveStrategy;
import org.junit.Before;

/**
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ImportWithFullInputConfigurationTest {

    private OTeleporterContext context;
    private ODBMSNaiveStrategy naiveStrategy;
    private ODBMSNaiveAggregationStrategy naiveAggregationStrategy;
    private String                        outOrientGraphUri;
    private String                        dbParentDirectoryPath;
    private final String config = "src/test/resources/configuration-mapping/full-configuration-mapping.json";

    @Before
    public void init() {
        this.context = new OTeleporterContext();
        this.context.setOutputManager(new OOutputStreamManager(0));
        this.context.setNameResolver(new OJavaConventionNameResolver());
        this.context.setDataTypeHandler(new OHSQLDBDataTypeHandler());
        this.naiveStrategy = new ODBMSNaiveStrategy();
        this.naiveAggregationStrategy = new ODBMSNaiveAggregationStrategy();
        this.outOrientGraphUri = "plocal:target/testOrientDB";
        this.dbParentDirectoryPath = this.outOrientGraphUri.replace("plocal:","");
    }


}
