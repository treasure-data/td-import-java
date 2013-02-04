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
    // PREPARE_PARTS_OPTIONS              //
    ////////////////////////////////////////

    // format [csv, tsv, json, msgpack, apache, regexp]; default=auto detect
    String BI_PREPARE_PARTS_FORMAT = "td.bulk_import.prepare_parts.format";
    String BI_PREPARE_PARTS_FORMAT_DEFAULTVALUE = "csv"; // default 'csv'

    // compress [gzip,.., auto]; default=auto detect
    String BI_PREPARE_PARTS_COMPRESS = "td.bulk_import.prepare_parts.compress";
    String BI_PREPARE_PARTS_COMPRESS_DEFAULTVALUE = "auto";

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

    String BI_PREPARE_PARTS_SAMPLE_ROWSIZE = "td.bulk_import.prepare_parts.sample.rowsize";
    String BI_PREPARE_PARTS_SAMPLE_ROWSIZE_DEFAULTVALUE = "30";

    String BI_PREPARE_PARTS_SPLIT_SIZE = "td.bulk_import.prepare_parts.split-size";
    String BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE ="16384";

    ////////////////////////////////////////
    // CSV/TSV_OPTIONS                    //
    ////////////////////////////////////////

    // delimiter CHAR; default=',' at 'csv', '\t' at 'tsv'
    String BI_PREPARE_PARTS_DELIMITER = "td.bulk_import.prepare_parts.delimiter";
    String BI_PREPARE_PARTS_DELIMITER_CSV_DEFAULTVALUE = ",";
    String BI_PREPARE_PARTS_DELIMITER_TSV_DEFAULTVALUE = "\t";

    // newline [CRLF, LF, CR]; default=CRLF (or auto detect?)
    String BI_PREPARE_PARTS_NEWLINE = "td.bulk_import.prepare_parts.newline";
    String BI_PREPARE_PARTS_NEWLINE_DEFAULTVALUE = "\r\n"; // default CRLF

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

}
