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
package com.treasure_data.bulk_import;

public interface Constants extends com.treasure_data.client.Constants {

    String CMD_PREPARE_PARTS = "prepare_parts";

    String CMD_UPLOAD_PARTS = "upload_parts";


    ////////////////////////////////////////
    // UPLOAD_PARTS_OPTIONS               //
    ////////////////////////////////////////

    // auto-perform
    String BI_UPLOAD_PARTS_AUTO_PERFORM = "td.bulk_import.upload_parts.auto-perform";
    String BI_UPLOAD_PARTS_AUTO_PERFORM_DEFAULTVALUE = "true";

    // auto-commit
    String BI_UPLOAD_PARTS_AUTO_COMMIT = "td.bulk_import.upload_parts.auto-commit";
    String BI_UPLOAD_PARTS_AUTO_COMMIT_DEFAULTVALUE = "false";

    // parallel NUM (deprecated TODO)
    String BI_UPLOAD_PARTS_PARALLEL = "td.bulk_import.upload_parts.upload-parallel";
    String BI_UPLOAD_PARTS_PARALLEL_DEFAULTVALUE = "2";
    String BI_UPLOAD_PARTS_PARALLEL_MAX_VALUE = "8";

    // retryCount NUM
    String BI_UPLOAD_PARTS_RETRYCOUNT = "td.bulk_import.upload_parts.retrycount";
    String BI_UPLOAD_PARTS_RETRYCOUNT_DEFAULTVALUE = "10";

    // waitSec NUM
    String BI_UPLOAD_PARTS_WAITSEC = "td.bulk_import.upload_parts.waitsec";
    String BI_UPLOAD_PARTS_WAITSEC_DEFAULTVALUE = "1";
}
