package com.treasure_data.commands.bulk_import;

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.Config;

public class CSVPreparePartsRequest extends PreparePartsRequest {
    private static final Logger LOG = Logger
            .getLogger(CSVPreparePartsRequest.class.getName());

    protected char delimiterChar;
    protected String newline;
    protected String[] columnNames;
    protected String[] columnTypeHints;
    protected boolean hasColumnHeader;
    protected String typeErrorMode;
    protected String[] excludeColumns;
    protected String[] onlyColumns;

    public CSVPreparePartsRequest() throws CommandException {
        super();
    }

    public CSVPreparePartsRequest(String format, String[] fileNames,
            Properties props) throws CommandException {
        super(format, fileNames, props);
    }

    @Override
    public void setOptions(Properties props) throws CommandException {
        super.setOptions(props);

        // delimiter
        if (format.equals("csv")) {
            delimiterChar = props.getProperty(
                    Config.BI_PREPARE_PARTS_DELIMITER,
                    Config.BI_PREPARE_PARTS_DELIMITER_CSV_DEFAULTVALUE)
                    .charAt(0);
        } else { // "tsv"
            delimiterChar = props.getProperty(
                    Config.BI_PREPARE_PARTS_DELIMITER,
                    Config.BI_PREPARE_PARTS_DELIMITER_TSV_DEFAULTVALUE)
                    .charAt(0);
        }

        // newline
        newline = props.getProperty(Config.BI_PREPARE_PARTS_NEWLINE,
                Config.BI_PREPARE_PARTS_NEWLINE_DEFAULTVALUE);

        boolean setColumns = false;

        // columns
        String cs = props.getProperty(Config.BI_PREPARE_PARTS_COLUMNS);
        if (cs != null) {
            setColumns = true;
            columnNames = cs.split(",");
        }

        // column header
        String columnHeader = props
                .getProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER);
        if (columnHeader == null || !columnHeader.equals("true")) {
            if (!setColumns) {
                throw new CommandException("Column names not set");
            }
            hasColumnHeader = false;
        } else {
            hasColumnHeader = true;
        }

        // column types
        String ctypes = props
                .getProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES);
        if (ctypes == null || ctypes.isEmpty()) {
            throw new CommandException("Column types is required: "
                    + Config.BI_PREPARE_PARTS_COLUMNTYPES);
        } else {
            columnTypeHints = ctypes.split(",");
        }

        // type-conversion-error
        typeErrorMode = props.getProperty(
                Config.BI_PREPARE_PARTS_TYPE_CONVERSION_ERROR,
                Config.BI_PREPARE_PARTS_TYPE_CONVERSION_ERROR_DEFAULTVALUE);

        // exclude-columns
        String ecs = props
                .getProperty(Config.BI_PREPARE_PARTS_EXCLUDE_COLUMNS);
        if (ecs != null) {
            excludeColumns = ecs.split(",");
        }

        // only-columns
        String ocs = props
                .getProperty(Config.BI_PREPARE_PARTS_ONLY_COLUMNS);
        if (ocs != null) {
            onlyColumns = ocs.split(",");
        }
    }

    public char getDelimiterChar() {
        return delimiterChar;
    }

    public String getNewline() {
        return newline;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public String[] getColumnTypeHints() {
        return columnTypeHints;
    }

    public boolean hasColumnHeader() {
        return hasColumnHeader;
    }

    public String getTypeErrorMode() {
        return typeErrorMode;
    }

    public String[] getExcludeColumns() {
        return excludeColumns;
    }

    public String[] getOnlyColumns() {
        return onlyColumns;
    }
}
