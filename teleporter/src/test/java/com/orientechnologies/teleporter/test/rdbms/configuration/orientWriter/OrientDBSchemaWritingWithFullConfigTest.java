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

package com.orientechnologies.teleporter.test.rdbms.configuration.orientWriter;

import com.orientechnologies.teleporter.context.OOutputStreamManager;
import com.orientechnologies.teleporter.context.OTeleporterContext;
import com.orientechnologies.teleporter.mapper.rdbms.OER2GraphMapper;
import com.orientechnologies.teleporter.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.teleporter.writer.OGraphModelWriter;
import org.junit.Before;

/**
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 *
 */

public class OrientDBSchemaWritingWithFullConfigTest {

    private OER2GraphMapper mapper;
    private OTeleporterContext context;
    private OGraphModelWriter modelWriter;
    private String             outOrientGraphUri;
    private final String config = "src/test/resources/configuration-mapping/full-configuration-mapping.json";

    @Before
    public void init() {
        this.context = new OTeleporterContext();
        this.context.setOutputManager(new OOutputStreamManager(0));
        this.context.setNameResolver(new OJavaConventionNameResolver());
        this.context.setQueryQuoteType("\"");
        this.modelWriter = new OGraphModelWriter();
        this.outOrientGraphUri = "memory:testOrientDB";
    }


}
