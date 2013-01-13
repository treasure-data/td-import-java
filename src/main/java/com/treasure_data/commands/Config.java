package com.treasure_data.commands;

public interface Config {

    String BI_PREPARE_PARTS_FORMAT = "td.bulk_import.preparee_parts.format";

    String BI_PREPARE_PARTS_COLUMNS = "td.bulk_import.prepare_parts.columns";

    String BI_PREPARE_PARTS_COLUMNHEADER = "td.bulk_import.prepare_parts.column-header";

    String BI_PREPARE_PARTS_COLUMNTYPES = "td.bulk_import.prepare_parts.column-types";

    String BI_PREPARE_PARTS_TIMECOLUMN = "td.bulk_import.prepare_parts.time-column";
    String BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE = "time";

    String BI_PREPARE_PARTS_OUTPUTDIR = "td.bulk_import.prepare_parts.output-dir";
    String BI_PREPARE_PARTS_OUTPUTDIR_DEFAULTVALUE = ".";

    String BI_PREPARE_PARTS_SPLIT_SIZE = "td.bulk_import.prepare_parts.split-size";
    String BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE ="16384";
}
