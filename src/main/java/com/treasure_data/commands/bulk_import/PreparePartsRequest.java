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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.CommandRequest;
import com.treasure_data.commands.Config;

public class PreparePartsRequest extends CommandRequest {
    private static final Logger LOG = Logger
            .getLogger(PreparePartsRequest.class.getName());

    private static final String COMMAND_NAME = "prepare_parts";

    protected File[] files;

    protected String format;
    protected String compressType;
    protected String encoding;
    protected String aliasTimeColumn;
    protected long timeValue = -1;
    protected String timeFormat;
    protected String errorRecordOutputDirName;
    protected boolean dryRun = false;
    protected int sampleHintScore;
    protected int sampleRowSize;
    protected String outputDirName;
    protected int splitSize;

    public PreparePartsRequest() throws CommandException {
        super(null);
    }

    public PreparePartsRequest(String format, String[] fileNames, Properties props)
            throws CommandException {
        super(props);
        setFormat(format);
        setFiles(fileNames);
        setOptions(getProperties());
    }

    void setFormat(String format) {
        this.format = format;
    }

    @Override
    public String getName() {
        return COMMAND_NAME;
    }

    protected void setFiles(String[] fileNames) throws CommandException {
        // validation for file names
        List<File> fileList = new ArrayList<File>(fileNames.length);
        for (int i = 0; i < fileNames.length; i++) {
            String fname = fileNames[i];
            File f = new File(fname);
            if (!f.isFile()) {
                LOG.severe("No such file: " + fname);
            } else {
                fileList.add(f);
            }
        }
        files = fileList.toArray(new File[0]);
    }

    public File[] getFiles() {
        return files;
    }

    protected void setOptions(Properties props) throws CommandException {
        // compress
        compressType = props.getProperty(Config.BI_PREPARE_PARTS_COMPRESS,
                Config.BI_PREPARE_PARTS_COMPRESS_DEFAULTVALUE);

        // encoding
        encoding = props.getProperty(Config.BI_PREPARE_PARTS_ENCODING,
                Config.BI_PREPARE_PARTS_ENCODING_DEFAULTVALUE);

        // time column
        String tc = props.getProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN);
        if (tc != null) {
            aliasTimeColumn = tc;
        }

        // time column value
        String tv = props.getProperty(Config.BI_PREPARE_PARTS_TIMEVALUE);
        if (tv != null) {
            timeValue = Long.parseLong(tv);
        }

        // time format
        timeFormat = props.getProperty(Config.BI_PREPARE_PARTS_TIMEFORMAT,
                Config.BI_PREPARE_PARTS_TIMEFORMAT_DEFAULTVALUE);

        // output DIR
        outputDirName = props.getProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR);
        if (outputDirName == null || outputDirName.isEmpty()) {
            throw new CommandException("Output dir is required: "
                    + Config.BI_PREPARE_PARTS_OUTPUTDIR);
        }

        // error record output DIR
        errorRecordOutputDirName = props
                .getProperty(Config.BI_PREPARE_PARTS_ERROR_RECORD_OUTPUT);

        // dry-run mode
        String drun = props.getProperty(Config.BI_PREPARE_PARTS_DRYRUN,
                Config.BI_PREPARE_PARTS_DRYRUN_DEFAULTVALUE);
        dryRun = drun != null && drun.equals("true");

        // row size with sample reader
        String srs = props.getProperty(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE,
                Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE_DEFAULTVALUE);
        if (srs != null) {
            sampleRowSize = Integer.parseInt(srs);
        }

        // hint score with sample reader
        String hscore = props.getProperty(Config.BI_PREPARE_PARTS_SAMPLE_HINT_SCORE,
                Config.BI_PREPARE_PARTS_SAMPLE_HINT_SCORE_DEFAULTVALUE);
        if (hscore != null) {
            sampleHintScore = Integer.parseInt(hscore);
        }

        // split size
        String splitsize = props.getProperty(
                Config.BI_PREPARE_PARTS_SPLIT_SIZE,
                Config.BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE);
        splitSize = Integer.parseInt(splitsize);
    }

    public String getFormat() {
        return format;
    }

    public String getCompressType() {
        return compressType;
    }

    public String getEncoding() {
        return encoding;
    }

    public String getAliasTimeColumn() {
        return aliasTimeColumn;
    }

    public long getTimeValue() {
        return timeValue;
    }
    public String getTimeFormat() {
        return timeFormat;
    }

    public String getErrorRecordOutputDirName() {
        return errorRecordOutputDirName;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public int getSampleHintScore() {
        return sampleHintScore;
    }

    public int getSampleRowSize() {
        return sampleRowSize;
    }

    public String getOutputDirName() {
        return outputDirName;
    }

    public int getSplitSize() {
        return splitSize;
    }
}
