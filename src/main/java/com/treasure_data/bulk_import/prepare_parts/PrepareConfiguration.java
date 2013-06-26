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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.model.ColumnType;
import com.treasure_data.bulk_import.reader.CSVFileReader;
import com.treasure_data.bulk_import.reader.FileReader;
import com.treasure_data.bulk_import.reader.MySQLTableReader;
import com.treasure_data.bulk_import.writer.FileWriter;
import com.treasure_data.bulk_import.writer.MsgpackGZIPFileWriter;

public class PrepareConfiguration extends Configuration {

    public static enum Format {
        // TODO #MN should consider type parameters
        CSV("csv") {
            @Override
            public FileReader createFileReader(PrepareConfiguration conf, FileWriter writer)
                    throws PreparePartsException {
                return new CSVFileReader(conf, writer);
            }
        },
        TSV("tsv") {
            @Override
            public FileReader createFileReader(PrepareConfiguration conf, FileWriter writer)
                    throws PreparePartsException {
                return new CSVFileReader(conf, writer);
            }
        },
        MYSQL("mysql") {
            @Override
            public FileReader createFileReader(PrepareConfiguration conf, FileWriter writer)
                    throws PreparePartsException {
                return new MySQLTableReader(conf, writer);
            }
        },
        JSON("json") {
        },
        MSGPACK("msgpack") {
        };

        private String format;

        Format(String format) {
            this.format = format;
        }

        public String format() {
            return format;
        }

        public FileReader createFileReader(PrepareConfiguration conf, FileWriter writer)
                throws PreparePartsException {
            throw new PreparePartsException(
                    new UnsupportedOperationException("format: " + this));
        }

        public static Format fromString(String format) {
            return StringToFormat.get(format);
        }

        private static class StringToFormat {
            private static final Map<String, Format> REVERSE_DICTIONARY;

            static {
                Map<String, Format> map = new HashMap<String, Format>();
                for (Format elem : Format.values()) {
                    map.put(elem.format(), elem);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static Format get(String key) {
                return REVERSE_DICTIONARY.get(key);
            }
        }
    }

    public static enum OutputFormat {
        MSGPACKGZ("msgpackgz") {
            @Override
            public FileWriter createFileWriter(PrepareConfiguration conf) throws PreparePartsException {
                return new MsgpackGZIPFileWriter(conf);
            }
        };

        private String outputFormat;

        OutputFormat(String outputFormat) {
            this.outputFormat = outputFormat;
        }

        public String outputFormat() {
            return outputFormat;
        }

        public FileWriter createFileWriter(PrepareConfiguration conf) throws PreparePartsException {
            throw new PreparePartsException(
                    new UnsupportedOperationException("output format: " + this));
        }

        public static OutputFormat fromString(String outputFormat) {
            return StringToOutputFormat.get(outputFormat);
        }

        private static class StringToOutputFormat {
            private static final Map<String, OutputFormat> REVERSE_DICTIONARY;

            static {
                Map<String, OutputFormat> map = new HashMap<String, OutputFormat>();
                for (OutputFormat elem : OutputFormat.values()) {
                    map.put(elem.outputFormat(), elem);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static OutputFormat get(String key) {
                return REVERSE_DICTIONARY.get(key);
            }
        }
    }

    public static enum CompressionType {
        GZIP("gzip") {
            @Override
            public InputStream createInputStream(InputStream in) throws IOException {
                return new BufferedInputStream(new GZIPInputStream(in));
            }
        }, AUTO("auto") {
            @Override
            public InputStream createInputStream(InputStream in) throws IOException {
                throw new IOException("unsupported compress type");
            }
        }, NONE("none") {
            @Override
            public InputStream createInputStream(InputStream in) throws IOException {
                return new BufferedInputStream(in);
            }
        };

        private String type;

        CompressionType(String type) {
            this.type = type;
        }

        public String type() {
            return type;
        }

        public abstract InputStream createInputStream(InputStream in) throws IOException;

        public static CompressionType fromString(String type) {
            return StringToCompressionType.get(type);
        }

        private static class StringToCompressionType {
            private static final Map<String, CompressionType> REVERSE_DICTIONARY;

            static {
                Map<String, CompressionType> map = new HashMap<String, CompressionType>();
                for (CompressionType elem : CompressionType.values()) {
                    map.put(elem.type(), elem);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static CompressionType get(String key) {
                return REVERSE_DICTIONARY.get(key);
            }
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
            .getLogger(PrepareConfiguration.class.getName());

    // FIXME this field is also declared in td-client.Config.
    protected Properties props;

    protected Format format;
    protected OutputFormat outputFormat = OutputFormat.MSGPACKGZ;
    protected CompressionType compressionType;
    protected CharsetDecoder charsetDecoder;
    protected int numOfPrepareThreads;
    protected String aliasTimeColumn;
    protected long timeValue = -1;
    protected String timeFormat;
    protected String errorRecordOutputDirName;
    protected boolean dryRun = false;
    protected String outputDirName;
    protected int splitSize;

    protected char delimiterChar;
    protected NewLine newline;
    protected String[] columnNames;
    protected ColumnType[] columnTypes;
    protected boolean hasColumnHeader;
    protected String typeErrorMode;
    protected String[] excludeColumns;
    protected String[] onlyColumns;
    protected int sampleRowSize;

    protected String[] keys;
    protected ColumnType[] valueTypes;

    protected String jdbcConnectionURL;
    protected String jdbcUser;
    protected String jdbcPassword;
    protected String jdbcTable;

    public PrepareConfiguration() {
    }

    public void configure(Properties props) {
        this.props = props;

        // format
        setFormat();

        // output format
        setOutputFormat();

        // compression type
        setCompressionType();

        // parallel
        String pthreadNum = props.getProperty(BI_PREPARE_PARTS_PARALLEL,
                BI_PREPARE_PARTS_PARALLEL_DEFAULTVALUE);
        try {
            int n = Integer.parseInt(pthreadNum);
            if (n < 0) {
                numOfPrepareThreads = 2;
            } else if (n > 9){
                numOfPrepareThreads = 8;
            } else {
                numOfPrepareThreads = n;
            }
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "'int' value is required as 'parallel' option e.g. -D%s=5",
                    BI_UPLOAD_PARTS_PARALLEL);
            throw new IllegalArgumentException(msg, e);
        }

        // encoding
        String encoding = props.getProperty(Configuration.BI_PREPARE_PARTS_ENCODING,
                Configuration.BI_PREPARE_PARTS_ENCODING_DEFAULTVALUE);
        try {
            createCharsetDecoder(encoding);
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        // time column
        aliasTimeColumn = props.getProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN);

        // time column value
        String tValue = props.getProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE);
        if (tValue != null) {
            try {
                timeValue = Long.parseLong(tValue);
            } catch (NumberFormatException e) {
                String msg = String.format(
                        "time value is required as long type (unix timestamp) e.g. -D%s=1360141200",
                        Configuration.BI_PREPARE_PARTS_TIMEVALUE);
                throw new IllegalArgumentException(msg, e);
            }
        }

        // time format
        timeFormat = props.getProperty(Configuration.BI_PREPARE_PARTS_TIMEFORMAT);

        // output DIR
        outputDirName = props.getProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR,
                Configuration.BI_PREPARE_PARTS_OUTPUTDIR_DEFAULTVALUE);

        // error record output DIR
        errorRecordOutputDirName = props
                .getProperty(Configuration.BI_PREPARE_PARTS_ERROR_RECORD_OUTPUT);

        // dry-run mode
        String drun = props.getProperty(Configuration.BI_PREPARE_PARTS_DRYRUN,
                Configuration.BI_PREPARE_PARTS_DRYRUN_DEFAULTVALUE);
        dryRun = drun != null && drun.equals("true");

        // split size
        String sSize = props.getProperty(
                Configuration.BI_PREPARE_PARTS_SPLIT_SIZE,
                Configuration.BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE);
        try {
            splitSize = Integer.parseInt(sSize);
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "split size is required as int type e.g. -D%s=%s",
                    Configuration.BI_PREPARE_PARTS_SPLIT_SIZE,
                    Configuration.BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE);
            throw new IllegalArgumentException(msg, e);
        }

        // delimiter
        if (format.equals(PrepareConfiguration.Format.CSV)) {
            delimiterChar = props.getProperty(
                    Configuration.BI_PREPARE_PARTS_DELIMITER,
                    Configuration.BI_PREPARE_PARTS_DELIMITER_CSV_DEFAULTVALUE).charAt(
                    0);
        } else if (format.equals(PrepareConfiguration.Format.TSV)) {
            delimiterChar = props.getProperty(
                    Configuration.BI_PREPARE_PARTS_DELIMITER,
                    Configuration.BI_PREPARE_PARTS_DELIMITER_TSV_DEFAULTVALUE).charAt(
                    0);
        } else {
            // fatal error. i mean here might be not executed
            throw new IllegalArgumentException("unsupported format: " + format);
        }
        LOG.config(String.format("use '%s' as delimiterChar", delimiterChar));

        // newline
        String nLine = props.getProperty(Configuration.BI_PREPARE_PARTS_NEWLINE,
                Configuration.BI_PREPARE_PARTS_NEWLINE_DEFAULTVALUE);
        try {
            newline = NewLine.valueOf(nLine);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unsupported newline char: " + nLine, e);
        }
        LOG.config(String.format("use '%s' as newline", newline));

        // column header
        String columnHeader = props.getProperty(
                Configuration.BI_PREPARE_PARTS_COLUMNHEADER,
                Configuration.BI_PREPARE_PARTS_COLUMNHEADER_DEFAULTVALUE);
        if (!columnHeader.equals("true")) {
            // columns
            String columns = props.getProperty(
                    Configuration.BI_PREPARE_PARTS_COLUMNS);
            if (columns != null && !columns.isEmpty()) {
                columnNames = columns.split(",");
            } else {
                throw new IllegalArgumentException("Column names not set");
            }
            hasColumnHeader = false;
        } else {
            hasColumnHeader = true;
        }

        setColumnTypes();

        // type-conversion-error
        typeErrorMode = props.getProperty(
                Configuration.BI_PREPARE_PARTS_TYPE_CONVERSION_ERROR,
                Configuration.BI_PREPARE_PARTS_TYPE_CONVERSION_ERROR_DEFAULTVALUE);

        // exclude-columns
        String excludeColumns = props.getProperty(
                Configuration.BI_PREPARE_PARTS_EXCLUDE_COLUMNS);
        if (excludeColumns != null && !excludeColumns.isEmpty()) {
            this.excludeColumns = excludeColumns.split(",");
            for (String c : this.excludeColumns) {
                if (c.equals(Configuration.BI_PREPARE_PARTS_TIMECOLUMN)) {
                    throw new IllegalArgumentException(
                            "'time' column cannot be included in excluded columns");
                }
            }
        } else {
            this.excludeColumns = new String[0];
        }

        // only-columns
        String onlyColumns = props.getProperty(Configuration.BI_PREPARE_PARTS_ONLY_COLUMNS);
        if (onlyColumns != null && !onlyColumns.isEmpty()) {
            this.onlyColumns = onlyColumns.split(",");
            for (String oc : this.onlyColumns) {
                for (String ec : this.excludeColumns) {
                    if (oc.equals(ec)) {
                        throw new IllegalArgumentException(
                                "'exclude' columns include specified 'only' columns");
                    }
                }
            }
        } else {
            this.onlyColumns = new String[0];
        }

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

    public void setFormat() {
        String formatStr = props.getProperty(
                Configuration.BI_PREPARE_PARTS_FORMAT,
                Configuration.BI_PREPARE_PARTS_FORMAT_DEFAULTVALUE);
        format = Format.fromString(formatStr);
        if (format == null) {
            throw new IllegalArgumentException(String.format(
                    "unsupported format '%s'", formatStr));
        }
    }

    public Format getFormat() {
        return format;
    }

    public void setOutputFormat() {
        String outputFormatStr = props.getProperty(
                Configuration.BI_PREPARE_PARTS_OUTPUTFORMAT,
                Configuration.BI_PREPARE_PARTS_OUTPUTFORMAT_DEFAULTVALUE);
        outputFormat = OutputFormat.fromString(outputFormatStr);
        if (outputFormat == null) {
            throw new IllegalArgumentException(String.format(
                    "unsupported format '%s'", outputFormatStr));
        }
    }

    public OutputFormat getOutputFormat() {
        return outputFormat;
    }

    public void setCompressionType() {
        String compType = props.getProperty(
                Configuration.BI_PREPARE_PARTS_COMPRESSION,
                Configuration.BI_PREPARE_PARTS_COMPRESSION_DEFAULTVALUE);
        compressionType = CompressionType.fromString(compType);
        if (compressionType == null) {
            throw new IllegalArgumentException(String.format(
                    "unsupported compression type: %s", compressionType));
        }
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public CompressionType checkCompressionType(String fileName) throws PreparePartsException {
        if (getCompressionType() != CompressionType.AUTO) {
            return getCompressionType();
        }

        CompressionType[] candidateCompressTypes = new CompressionType[] {
                CompressionType.GZIP, CompressionType.NONE,
        };

        CompressionType compressionType = null;
        for (int i = 0; i < candidateCompressTypes.length; i++) {
            InputStream in = null;
            try {
                if (candidateCompressTypes[i].equals(CompressionType.GZIP)) {
                    in = CompressionType.GZIP.createInputStream(new FileInputStream(fileName));
                } else if (candidateCompressTypes[i].equals(CompressionType.NONE)) {
                    in = CompressionType.NONE.createInputStream(new FileInputStream(fileName));
                } else {
                    throw new PreparePartsException("fatal error");
                }
                byte[] b = new byte[2];
                in.read(b);

                compressionType = candidateCompressTypes[i];
                break;
            } catch (IOException e) {
                LOG.fine(String.format("file %s is %s", fileName,
                        e.getMessage()));
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }

        this.compressionType = compressionType;
        return compressionType;
    }

    public int getNumOfPrepareThreads() {
        return numOfPrepareThreads;
    }

    public void createCharsetDecoder(String encoding) throws Exception {
        charsetDecoder = Charset.forName(encoding).newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
    }

    public CharsetDecoder getCharsetDecoder() throws PreparePartsException {
        return charsetDecoder;
    }

    public String getAliasTimeColumn() {
        return aliasTimeColumn;
    }

    public long getTimeValue() {
        return timeValue;
    }

    public ExtStrftime getTimeFormat() {
        return timeFormat == null ? null : new ExtStrftime(timeFormat);
    }

    public String getErrorRecordOutputDirName() {
        return errorRecordOutputDirName;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public String getOutputDirName() {
        return outputDirName;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public char getQuoteChar() {
        return '"';
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

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnTypes() {
        String types = props.getProperty(Configuration.BI_PREPARE_PARTS_COLUMNTYPES);
        if (types != null && !types.isEmpty()) {
            String[] splited = types.split(",");
            columnTypes = new ColumnType[splited.length];
            for (int i = 0; i < columnTypes.length; i++) {
                columnTypes[i] = ColumnType.fromString(splited[i].toLowerCase());
            }
        } else {
            columnTypes = new ColumnType[0];
        }
    }

    public void setColumnTypes(ColumnType[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public ColumnType[] getColumnTypes() {
        return columnTypes;
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

    public int getSampleRowSize() {
        return sampleRowSize;
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
