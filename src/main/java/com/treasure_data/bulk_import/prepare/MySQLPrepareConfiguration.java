//
// Treasure Data Bulk-Import Tool in Java
//
// Copyright (C) 2012 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.bulk_import.prepare;

import java.util.List;
import java.util.Properties;

import com.treasure_data.bulk_import.BulkImportOptions;

public class MySQLPrepareConfiguration extends PrepareConfiguration {

    protected String jdbcUrl;
    protected String user;
    protected String password;
    protected String table;

    @Override
    public void configure(Properties props, BulkImportOptions options) {
        super.configure(props, options);

        setJdbcUrl();
        setUser();
        setPassword();
    }

    public void setJdbcUrl() {
        if (!optionSet.has(BI_PREPARE_PARTS_JDBC_CONNECTION_URL)) {
            throw new IllegalArgumentException("Not specified connection URL");
        }
        jdbcUrl = (String) optionSet.valueOf(BI_PREPARE_PARTS_JDBC_CONNECTION_URL);
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setUser() {
        if (!optionSet.has(BI_PREPARE_PARTS_JDBC_USER)) {
            throw new IllegalArgumentException("Not specified user");
        }
        user = (String) optionSet.valueOf(BI_PREPARE_PARTS_JDBC_USER);
    }

    public String getUser() {
        return user;
    }

    public void setPassword() {
        if (optionSet.has(BI_PREPARE_PARTS_JDBC_PASSWORD)) {
            password = (String) optionSet.valueOf(BI_PREPARE_PARTS_JDBC_PASSWORD);
        }
    }

    public String getPassword() {
        return password;
    }

    @Override
    public List<String> getNonOptionArguments() {
        List<String> argList = super.getNonOptionArguments();
        if (argList.size() > 2) {
            throw new IllegalArgumentException("Must not specified more than one table name at a time");
        }
        return argList;
    }
}
