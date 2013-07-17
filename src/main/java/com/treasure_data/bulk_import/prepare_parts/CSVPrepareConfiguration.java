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
import java.util.logging.Logger;

import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.model.ColumnType;

public class CSVPrepareConfiguration extends PrepareConfiguration {

    public static enum Quote {
        DOUBLE("\""), SINGLE("'");

        private String quote;

        Quote(String quote) {
            this.quote = quote;
        }

        public char quote() {
            return quote.charAt(0);
        }
    }

    public static enum NewLine {
        CR("\r"), LF("\n"), CRLF("\r\n");

        private String newline;

        NewLine(String newline) {
            this.newline = newline;
        }

        public String newline() {
            return newline;
        }
    }

    private static final Logger LOG = Logger
            .getLogger(CSVPrepareConfiguration.class.getName());

    protected char delimiterChar;
    protected Quote quoteChar;
    protected NewLine newline;
    protected boolean hasColumnHeader;
    protected String typeErrorMode;
    protected int sampleRowSize;

    public CSVPrepareConfiguration() {
        super();
    }

    @Override
    public void configure(Properties props) {
        super.configure(props);

        // delimiter
        if (format.equals(CSVPrepareConfiguration.Format.CSV)) {
            delimiterChar = props.getProperty(
                    Configuration.BI_PREPARE_PARTS_DELIMITER,
                    Configuration.BI_PREPARE_PARTS_DELIMITER_CSV_DEFAULTVALUE).charAt(
                    0);
        } else if (format.equals(CSVPrepareConfiguration.Format.TSV)) {
            delimiterChar = props.getProperty(
                    Configuration.BI_PREPARE_PARTS_DELIMITER,
                    Configuration.BI_PREPARE_PARTS_DELIMITER_TSV_DEFAULTVALUE).charAt(
                    0);
        }

        // quote
        String quote_char = props.getProperty(Configuration.BI_PREPARE_PARTS_QUOTE,
                Configuration.BI_PREPARE_PARTS_QUOTE_DEFAULTVALUE);
        try {
            quoteChar = Quote.valueOf(quote_char);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unsupported quote char: " + quote_char, e);
        }

        // newline
        String nLine = props.getProperty(Configuration.BI_PREPARE_PARTS_NEWLINE,
                Configuration.BI_PREPARE_PARTS_NEWLINE_DEFAULTVALUE);
        try {
            newline = NewLine.valueOf(nLine);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unsupported newline char: " + nLine, e);
        }

        setColumnHeader();

        setColumnNames();

        setColumnTypes();

        // type-conversion-error
        typeErrorMode = props.getProperty(
                Configuration.BI_PREPARE_PARTS_TYPE_CONVERSION_ERROR,
                Configuration.BI_PREPARE_PARTS_TYPE_CONVERSION_ERROR_DEFAULTVALUE);

        // row size with sample reader
        String sRowSize = props.getProperty(
                Configuration.BI_PREPARE_PARTS_SAMPLE_ROWSIZE,
                Configuration.BI_PREPARE_PARTS_SAMPLE_ROWSIZE_DEFAULTVALUE);
        try {
            sampleRowSize = Integer.parseInt(sRowSize);
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "sample row size is required as int type e.g. -D%s=%s",
                    Configuration.BI_PREPARE_PARTS_SAMPLE_ROWSIZE,
                    Configuration.BI_PREPARE_PARTS_SAMPLE_ROWSIZE_DEFAULTVALUE);
            throw new IllegalArgumentException(msg, e);
        }
    }

    public Quote getQuoteChar() {
        return quoteChar;
    }

    public char getDelimiterChar() {
        return delimiterChar;
    }

    public NewLine getNewline() {
        return newline;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public void setColumnHeader() {
        String columnHeader = props.getProperty(
                Configuration.BI_PREPARE_PARTS_COLUMNHEADER,
                Configuration.BI_PREPARE_PARTS_COLUMNHEADER_DEFAULTVALUE);
        if (!columnHeader.equals("true")) {
            hasColumnHeader = false;
        } else {
            hasColumnHeader = true;
        }
    }

    @Override
    public void setColumnNames() {
        String columns = props.getProperty(
                Configuration.BI_PREPARE_PARTS_COLUMNS);
        if (columns != null && !columns.isEmpty()) {
            columnNames = columns.split(",");
        } else if (!hasColumnHeader()) {
            throw new IllegalArgumentException("Column names not set");
        }
    }

    public boolean hasColumnHeader() {
        return hasColumnHeader;
    }

    public String getTypeErrorMode() {
        return typeErrorMode;
    }

    public int getSampleRowSize() {
        return sampleRowSize;
    }
}
