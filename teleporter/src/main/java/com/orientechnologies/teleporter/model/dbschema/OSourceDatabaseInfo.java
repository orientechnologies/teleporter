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

package com.orientechnologies.teleporter.model.dbschema;

import com.orientechnologies.teleporter.model.OSourceInfo;

/**
 * Represents a source database with all its related info for accessing it.
 */

public class OSourceDatabaseInfo implements OSourceInfo {

    private String sourceIdName;
    private String driverName;
    private String url;
    private String username;
    private String password;

    public OSourceDatabaseInfo(String sourceIdName, String driverName, String url, String username, String password) {
        this.sourceIdName = sourceIdName;
        this.driverName = driverName;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public String getSourceIdName() {
        return this.sourceIdName;
    }

    public void setSourceIdName(String sourceIdName) {
        this.sourceIdName = sourceIdName;
    }

    public String getDriverName() {
        return this.driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getUrl() {
        return this.url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public int hashCode() {
        int result = sourceIdName.hashCode();
        result = 31 * result + driverName.hashCode();
        result = 31 * result + url.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OSourceDatabaseInfo that = (OSourceDatabaseInfo) o;

        if (!sourceIdName.equals(that.sourceIdName)) return false;
        if (!driverName.equals(that.driverName)) return false;
        return url.equals(that.url);

    }
}
