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

    ////////////////////////////////////////
    // UPLOAD_PARTS_OPTIONS               //
    ////////////////////////////////////////

    // auto-perform
    String BI_UPLOAD_PARTS_AUTOPERFORM = "td.bulk_import.upload_parts.auto-perform";
    String BI_UPLOAD_PARTS_AUTOPERFORM_DEFAULTVALUE = "true";

    // auto-commit
    String BI_UPLOAD_PARTS_AUTOCOMMIT = "td.bulk_import.upload_parts.auto-commit";
    String BI_UPLOAD_PARTS_AUTOCOMMIT_DEFAULTVALUE = "false";

    // retryCount
    String BI_UPLOAD_PARTS_RETRYCOUNT = "td.bulk_import.upload_parts.retrycount";
    String BI_UPLOAD_PARTS_RETRYCOUNT_DEFAULTVALUE = "10";

    // waitSec
    String BI_UPLOAD_PARTS_WAITSEC = "td.bulk_import.upload_parts.waitsec";
    String BI_UPLOAD_PARTS_WAITSEC_DEFAULTVALUE = "1";

    ////////////////////////////////////////
    // PREPARE_PARTS_OPTIONS              //
    ////////////////////////////////////////

    // format [csv, tsv, json, msgpack, apache, regexp]; default=auto detect
    String BI_PREPARE_PARTS_FORMAT = "td.bulk_import.prepare_parts.format";
    String BI_PREPARE_PARTS_FORMAT_DEFAULTVALUE = "csv"; // default 'csv'

    // compress [gzip,.., auto]; default=auto detect
    String BI_PREPARE_PARTS_COMPRESSION = "td.bulk_import.prepare_parts.compression";
    String BI_PREPARE_PARTS_COMPRESSION_DEFAULTVALUE = "auto";

    // encoding [utf-8,...]
    String BI_PREPARE_PARTS_ENCODING = "td.bulk_import.prepare_parts.encoding";
    String BI_PREPARE_PARTS_ENCODING_DEFAULTVALUE = "utf-8";

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

    // error-record-output DIR; format=reason + line(percent-encoded?); default=NULL output stream
    // reason: type conversion error (->int, ->boolean, ->double, etc)
    // reason: ??
    String BI_PREPARE_PARTS_ERROR_RECORD_OUTPUT = "td.bulk_import.prepare_parts.error-record-output";

    // dry-run; show samples as JSON and exit
    String BI_PREPARE_PARTS_DRYRUN = "td.bulk_import.prepare_parts.dry-run";
    String BI_PREPARE_PARTS_DRYRUN_DEFAULTVALUE = "false";

    String BI_PREPARE_PARTS_SPLIT_SIZE = "td.bulk_import.prepare_parts.split-size";
    String BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE ="16384";

    ////////////////////////////////////////
    // CSV/TSV PREPARE_PARTS_OPTIONS      //
    ////////////////////////////////////////

    // delimiter CHAR; default=',' at 'csv', '\t' at 'tsv'
    String BI_PREPARE_PARTS_DELIMITER = "td.bulk_import.prepare_parts.delimiter";
    String BI_PREPARE_PARTS_DELIMITER_CSV_DEFAULTVALUE = ",";
    String BI_PREPARE_PARTS_DELIMITER_TSV_DEFAULTVALUE = "\t";

    // newline [CRLF, LF, CR]; default=CRLF (or auto detect?)
    String BI_PREPARE_PARTS_NEWLINE = "td.bulk_import.prepare_parts.newline";
    String BI_PREPARE_PARTS_NEWLINE_DEFAULTVALUE = "CRLF"; // default CRLF

    // column-header; default=true
    String BI_PREPARE_PARTS_COLUMNHEADER = "td.bulk_import.prepare_parts.column-header";
    String BI_PREPARE_PARTS_COLUMNHEADER_DEFAULTVALUE = "true";

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

    String BI_PREPARE_PARTS_SAMPLE_HINT_SCORE = "td.bulk_import.prepare_parts.sample.hint.score";
    String BI_PREPARE_PARTS_SAMPLE_HINT_SCORE_DEFAULTVALUE = "3";

    String BI_PREPARE_PARTS_SAMPLE_ROWSIZE = "td.bulk_import.prepare_parts.sample.rowsize";
    String BI_PREPARE_PARTS_SAMPLE_ROWSIZE_DEFAULTVALUE = "30";

    ////////////////////////////////////////
    // UPLOAD_PARTS_OPTIONS               //
    ////////////////////////////////////////

    // auto-perform
    String BI_UPLOAD_PARTS_AUTO_PERFORM = "td.bulk_import.upload_parts.auto-perform";
    String BI_UPLOAD_PARTS_AUTO_PERFORM_DEFAULTVALUE = "true";

    // auto-commit
    String BI_UPLOAD_PARTS_AUTO_COMMIT = "td.bulk_import.upload_parts.auto-commit";
    String BI_UPLOAD_PARTS_AUTO_COMMIT_DEFAULTVALUE = "false";

    // parallel NUM
    String BI_UPLOAD_PARTS_UPLOAD_PARALLEL = "td.bulk_import.upload_parts.upload-parallel";
    String BI_UPLOAD_PARTS_UPLOAD_PARALLEL_DEFAULTVALUE = "1"; // TODO #MN should change 2
    String BI_UPLOAD_PARTS_UPLOAD_PARALLEL_MAX_VALUE = "8"; // TODO #MN should change 2

}
