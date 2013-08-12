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

    // format
    String BI_UPLOAD_PARTS_FORMAT_DEFAULTVALUE = "msgpack.gz"; // default 'msgpack.gz'

    // auto-perform
    String BI_UPLOAD_PARTS_AUTO_PERFORM = "td.bulk_import.upload_parts.auto-perform";
    String BI_UPLOAD_PARTS_AUTO_PERFORM_DEFAULTVALUE = "false";

    // auto-commit
    String BI_UPLOAD_PARTS_AUTO_COMMIT = "td.bulk_import.upload_parts.auto-commit";
    String BI_UPLOAD_PARTS_AUTO_COMMIT_DEFAULTVALUE = "false";

    // parallel NUM
    String BI_UPLOAD_PARTS_PARALLEL = "td.bulk_import.upload_parts.upload-parallel";
    String BI_UPLOAD_PARTS_PARALLEL_DEFAULTVALUE = "2";
    String BI_UPLOAD_PARTS_PARALLEL_MAX_VALUE = "8";

    // retryCount NUM
    String BI_UPLOAD_PARTS_RETRYCOUNT = "td.bulk_import.upload_parts.retrycount";
    String BI_UPLOAD_PARTS_RETRYCOUNT_DEFAULTVALUE = "10";

    // waitSec NUM
    String BI_UPLOAD_PARTS_WAITSEC = "td.bulk_import.upload_parts.waitsec";
    String BI_UPLOAD_PARTS_WAITSEC_DEFAULTVALUE = "1";

    ////////////////////////////////////////
    // PREPARE_PARTS_OPTIONS              //
    ////////////////////////////////////////

    // format [csv, tsv, json, msgpack, apache, regexp]; default=auto detect
    String BI_PREPARE_PARTS_FORMAT = "td.bulk_import.prepare_parts.format";
    String BI_PREPARE_PARTS_FORMAT_DEFAULTVALUE = "csv"; // default 'csv'

    // output format [msgpackgz]; default=msgpackgz
    String BI_PREPARE_PARTS_OUTPUTFORMAT = "td.bulk_import.prepare_parts.outputformat";
    String BI_PREPARE_PARTS_OUTPUTFORMAT_DEFAULTVALUE = "msgpackgz";

    // compress [gzip,.., auto]; default=auto detect
    String BI_PREPARE_PARTS_COMPRESSION = "td.bulk_import.prepare_parts.compression";
    String BI_PREPARE_PARTS_COMPRESSION_DEFAULTVALUE = "auto";

    // parallel
    String BI_PREPARE_PARTS_PARALLEL = "td.bulk_import.prepare_parts.prepare-parallel";
    String BI_PREPARE_PARTS_PARALLEL_DEFAULTVALUE = "1";

    // encoding [utf-8,...]
    String BI_PREPARE_PARTS_ENCODING = "td.bulk_import.prepare_parts.encoding";
    String BI_PREPARE_PARTS_ENCODING_DEFAULTVALUE = "UTF-8";

    // time-column NAME; default='time'
    String BI_PREPARE_PARTS_TIMECOLUMN = "td.bulk_import.prepare_parts.time-column";
    String BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE = "time";

    // time-format STRF_FORMAT; default=auto detect
    String BI_PREPARE_PARTS_TIMEFORMAT = "td.bulk_import.prepare_parts.time-format";
    String BI_PREPARE_PARTS_TIMEFORMAT_DEFAULTVALUE = "auto";

    // time-value TIME; use fixed time value
    String BI_PREPARE_PARTS_TIMEVALUE = "td.bulk_import.prepare_parts.time-value";

    // output DIR
    String BI_PREPARE_PARTS_OUTPUTDIR = "td.bulk_import.prepare_parts.output-dir";
    String BI_PREPARE_PARTS_OUTPUTDIR_DEFAULTVALUE = "out"; // './out/'

    // error handling
    String BI_PREPARE_PARTS_ERROR_RECORDS_HANDLING = "td.bulk_import.prepare_parts.error-records-handling";
    String BI_PREPARE_PARTS_ERROR_RECORDS_HANDLING_DEFAULTVALUE= "skip";

    // dry-run; show samples as JSON and exit
    String BI_PREPARE_PARTS_DRYRUN = "td.bulk_import.prepare_parts.dry-run";
    String BI_PREPARE_PARTS_DRYRUN_DEFAULTVALUE = "false";

    String BI_PREPARE_PARTS_SPLIT_SIZE = "td.bulk_import.prepare_parts.split-size";
    String BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE ="16384";

    ////////////////////////////////////////
    // CSV/TSV PREPARE_PARTS_OPTIONS      //
    ////////////////////////////////////////

    // quote [DOUBLE, SINGLE]; default=DOUBLE
    String BI_PREPARE_PARTS_QUOTE = "td.bulk_import.prepare_parts.quote";
    String BI_PREPARE_PARTS_QUOTE_DEFAULTVALUE = "DOUBLE";

    // delimiter CHAR; default=',' at 'csv', '\t' at 'tsv'
    String BI_PREPARE_PARTS_DELIMITER = "td.bulk_import.prepare_parts.delimiter";
    String BI_PREPARE_PARTS_DELIMITER_CSV_DEFAULTVALUE = ",";
    String BI_PREPARE_PARTS_DELIMITER_TSV_DEFAULTVALUE = "\t";

    // newline [CRLF, LF, CR]; default=CRLF (or auto detect?)
    String BI_PREPARE_PARTS_NEWLINE = "td.bulk_import.prepare_parts.newline";
    String BI_PREPARE_PARTS_NEWLINE_DEFAULTVALUE = "CRLF"; // default CRLF

    // column-header; default=true
    String BI_PREPARE_PARTS_COLUMNHEADER = "td.bulk_import.prepare_parts.column-header";
    String BI_PREPARE_PARTS_COLUMNHEADER_DEFAULTVALUE = "false";

    // columns NAME,NAME,NAME,...; default=use column header
    String BI_PREPARE_PARTS_COLUMNS = "td.bulk_import.prepare_parts.columns";

    // column-types TYPE,TYPE,TYPE,...; default=auto detect
    String BI_PREPARE_PARTS_COLUMNTYPES = "td.bulk_import.prepare_parts.column-types";

    // type-conversion-error [skip,null]; default=skip
    String BI_PREPARE_PARTS_TYPE_CONVERSION_ERROR = "td.bulk_import.prepare_parts.type-conversion-error";
    String BI_PREPARE_PARTS_TYPE_CONVERSION_ERROR_DEFAULTVALUE = "skip";

    // exclude-columns NAME,NAME,NAME
    String BI_PREPARE_PARTS_EXCLUDE_COLUMNS = "td.bulk_import.prepare_parts.exclude-columns";
    // only-columns NAME,NAME,NAME
    String BI_PREPARE_PARTS_ONLY_COLUMNS = "td.bulk_import.prepare_parts.only-columns";

    String BI_PREPARE_PARTS_SAMPLE_ROWSIZE = "td.bulk_import.prepare_parts.sample.rowsize";
    String BI_PREPARE_PARTS_SAMPLE_ROWSIZE_DEFAULTVALUE = "30";

    ////////////////////////////////////////
    // MYSQL PREPARE_PARTS_OPTIONS        //
    ////////////////////////////////////////

    String BI_PREPARE_PARTS_MYSQL_JDBCDRIVER_CLASS = "com.mysql.jdbc.Driver";

    // url
    String BI_PREPARE_PARTS_JDBC_CONNECTION_URL = "td.bulk_import_prepare_parts.jdbc.connection.url";

    // table
    String BI_PREPARE_PARTS_JDBC_TABLE = "td.bulk_import_prepare_parts.jdbc.table";

    // user
    String BI_PREPARE_PARTS_JDBC_USER = "td.bulk_import_prepare_parts.jdbc.user";

    // password
    String BI_PREPARE_PARTS_JDBC_PASSWORD = "td.bulk_import_prepare_parts.jdbc.password";
}
