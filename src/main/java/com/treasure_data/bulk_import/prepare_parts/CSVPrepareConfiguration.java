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

import com.treasure_data.bulk_import.BulkImportOptions;
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
    public void configure(Properties props, BulkImportOptions options) {
        super.configure(props, options);

        // delimiter
        setDelimiterChar();

        // quote
        setQuoteChar();

        // newline
        setNewline();

        // column-header
        setColumnHeader();

        // column-names
        setColumnNames();

        // column-types
        setColumnTypes();

        // row size with sample reader
        setSampleReaderRowSize();
    }

    public void setDelimiterChar() {
        String delim;
        if (!optionSet.has("delimiter")) {
            if (format.equals(Format.CSV)) { // 'csv'
                delim = Configuration.BI_PREPARE_PARTS_DELIMITER_CSV_DEFAULTVALUE;
            } else { // 'tsv'
                delim = Configuration.BI_PREPARE_PARTS_DELIMITER_TSV_DEFAULTVALUE;
            }
        } else {
            delim = (String) optionSet.valueOf("delimiter");
        }
        delimiterChar = delim.charAt(0);
    }

    public char getDelimiterChar() {
        return delimiterChar;
    }

    public void setQuoteChar() {
        String quote;
        if (!optionSet.has("quote")) {
            quote = Configuration.BI_PREPARE_PARTS_QUOTE_DEFAULTVALUE;
        } else {
            quote = (String) optionSet.valueOf("quote");
        }

        try {
            quoteChar = Quote.valueOf(quote);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unsupported quote char: " + quote, e);
        }
    }

    public Quote getQuoteChar() {
        return quoteChar;
    }

    public void setNewline() {
        String nline;
        if (!optionSet.has("newline")) {
            nline = Configuration.BI_PREPARE_PARTS_NEWLINE_DEFAULTVALUE;
        } else {
            nline = (String) optionSet.valueOf("newline");
        }

        try {
            newline = NewLine.valueOf(nline);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unsupported newline char: " + nline, e);
        }
    }

    public NewLine getNewline() {
        return newline;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public void setColumnHeader() {
        hasColumnHeader = optionSet.has("column-header");
    }

    @Override
    public void setColumnNames() {
        if (optionSet.has("columns")) {
            columnNames = optionSet.valuesOf("columns").toArray(new String[0]);
        } else if (!hasColumnHeader()) {
            throw new IllegalArgumentException("Column names not set");
        }
    }

    public boolean hasColumnHeader() {
        return hasColumnHeader;
    }

    public void setSampleReaderRowSize() {
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

    public int getSampleRowSize() {
        return sampleRowSize;
    }

}
