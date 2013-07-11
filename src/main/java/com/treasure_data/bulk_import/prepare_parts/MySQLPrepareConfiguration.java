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
package com.treasure_data.bulk_import.prepare_parts;

import java.util.Properties;

public class MySQLPrepareConfiguration extends PrepareConfiguration {

    protected String jdbcConnectionURL;
    protected String jdbcUser;
    protected String jdbcPassword;
    protected String jdbcTable;

    @Override
    public void configure(Properties props) {
        super.configure(props);
    }

    public String getJDBCConnectionURL() {
        return jdbcConnectionURL;
    }

    public String getJDBCUser() {
        return jdbcUser;
    }

    public String getJDBCPassword() {
        return jdbcPassword;
    }

    public String getJDBCTable() {
        return jdbcTable;
    }
}
