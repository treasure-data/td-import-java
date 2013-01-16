//
// Java Extension to CUI for Treasure Data
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
package com.treasure_data.commands;

public interface Config {

    String BI_PREPARE_PARTS_FORMAT = "td.bulk_import.prepare_parts.format";

    String BI_PREPARE_PARTS_COLUMNS = "td.bulk_import.prepare_parts.columns";

    String BI_PREPARE_PARTS_COLUMNHEADER = "td.bulk_import.prepare_parts.column-header";

    String BI_PREPARE_PARTS_COLUMNTYPES = "td.bulk_import.prepare_parts.column-types";

    String BI_PREPARE_PARTS_TIMECOLUMN = "td.bulk_import.prepare_parts.time-column";
    String BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE = "time";

    String BI_PREPARE_PARTS_TIMEVALUE = "td.bulk_import.prepare_parts.time-value";

    String BI_PREPARE_PARTS_OUTPUTDIR = "td.bulk_import.prepare_parts.output-dir";

    String BI_PREPARE_PARTS_SPLIT_SIZE = "td.bulk_import.prepare_parts.split-size";
    String BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE ="16384";
}
