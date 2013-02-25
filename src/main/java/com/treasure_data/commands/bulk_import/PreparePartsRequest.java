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
package com.treasure_data.commands.bulk_import;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.CommandRequest;
import com.treasure_data.commands.Config;

public class PreparePartsRequest extends CommandRequest {
    public static enum Format {
        CSV("csv"), TSV("tsv"), JSON("json"), MSGPACK("msgpack");

        private String format;

        Format(String format) {
            this.format = format;
        }

        public String format() {
            return format;
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

    public static enum CompressionType {
        GZIP("gzip"), AUTO("auto"), NONE("none");

        private String type;

        CompressionType(String type) {
            this.type = type;
        }

        public String type() {
            return type;
        }

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

    private static final Logger LOG = Logger
            .getLogger(PreparePartsRequest.class.getName());

    private static final String COMMAND_NAME = "prepare_parts";

    protected Format format;
    protected CompressionType compressionType;
    protected String encoding;
    protected String aliasTimeColumn;
    protected long timeValue = -1;
    protected String timeFormat;
    protected String errorRecordOutputDirName;
    protected boolean dryRun = false;
    protected String outputDirName;
    protected int splitSize;

    public PreparePartsRequest() throws CommandException {
        super(null);
    }

    public PreparePartsRequest(Format format, String[] fileNames, Properties props)
            throws CommandException {
        super(props);
        setFormat(format);
        setFiles(fileNames);
        setOptions(getProperties());
    }

    @Override
    public String getName() {
        return COMMAND_NAME;
    }

    protected void setOptions(Properties props) throws CommandException {
        // compression type
        String compType = props.getProperty(
                Config.BI_PREPARE_PARTS_COMPRESSION,
                Config.BI_PREPARE_PARTS_COMPRESSION_DEFAULTVALUE);
        compressionType = CompressionType.fromString(compType);
        if (compressionType == null) {
            throw new CommandException("unsupported compression type: "
                    + compressionType);
        }

        // encoding
        encoding = props.getProperty(Config.BI_PREPARE_PARTS_ENCODING,
                Config.BI_PREPARE_PARTS_ENCODING_DEFAULTVALUE);

        // time column
        aliasTimeColumn = props.getProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN);

        // time column value
        String tValue = props.getProperty(Config.BI_PREPARE_PARTS_TIMEVALUE);
        if (tValue != null) {
            try {
                timeValue = Long.parseLong(tValue);
            } catch (NumberFormatException e) {
                String msg = String.format(
                        "time value is required as long type (unix timestamp) e.g. -D%s=1360141200",
                        Config.BI_PREPARE_PARTS_TIMEVALUE);
                throw new CommandException(msg, e);
            }
        }

        timeFormat = props.getProperty(Config.BI_PREPARE_PARTS_TIMEFORMAT,
                Config.BI_PREPARE_PARTS_TIMEFORMAT_DEFAULTVALUE);

        // output DIR
        outputDirName = props.getProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR);
        if (outputDirName == null || outputDirName.isEmpty()) {
            String msg = String.format("output dir is required e.g. -D%s=./",
                    Config.BI_PREPARE_PARTS_OUTPUTDIR);
            throw new CommandException(msg);
        }

        // error record output DIR
        errorRecordOutputDirName = props
                .getProperty(Config.BI_PREPARE_PARTS_ERROR_RECORD_OUTPUT);

        // dry-run mode
        String drun = props.getProperty(Config.BI_PREPARE_PARTS_DRYRUN,
                Config.BI_PREPARE_PARTS_DRYRUN_DEFAULTVALUE);
        dryRun = drun != null && drun.equals("true");

        // split size
        String sSize = props.getProperty(
                Config.BI_PREPARE_PARTS_SPLIT_SIZE,
                Config.BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE);
        try {
            splitSize = Integer.parseInt(sSize);
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "split size is required as int type e.g. -D%s=%s",
                    Config.BI_PREPARE_PARTS_SPLIT_SIZE,
                    Config.BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE);
            throw new CommandException(msg, e);
        }
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public Format getFormat() {
        return format;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setAliasTimeColumn(String aliasColumn) {
        this.aliasTimeColumn = aliasColumn;
    }

    public String getAliasTimeColumn() {
        return aliasTimeColumn;
    }

    public void setTimeValue(long timeValue) {
        // initial value of timeValue is '-1'
        this.timeValue = timeValue;
    }

    public long getTimeValue() {
        return timeValue;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setErrorRecordOutputDirName(String dirName) {
        this.errorRecordOutputDirName = dirName;
    }

    public String getErrorRecordOutputDirName() {
        return errorRecordOutputDirName;
    }

    public void setDryRun(boolean flag) {
        this.dryRun = flag;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public void setOutputDirName(String dirName) {
        this.outputDirName = dirName;
    }

    public String getOutputDirName() {
        return outputDirName;
    }

    public void setSplitSize(int size) {
        this.splitSize = size;
    }

    public int getSplitSize() {
        return splitSize;
    }
}
