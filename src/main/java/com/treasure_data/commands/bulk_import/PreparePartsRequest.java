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
import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.CommandRequest;
import com.treasure_data.commands.Config;

public class PreparePartsRequest extends CommandRequest {
    private static final Logger LOG = Logger
            .getLogger(PreparePartsRequest.class.getName());

    private static final String COMMAND_NAME = "prepare_parts";

    private File[] files;

    public PreparePartsRequest(String[] fileNames, Properties props)
            throws CommandException {
        super(props);
        setFiles(fileNames);
        setOptions(props);
    }

    @Override
    public String getName() {
        return COMMAND_NAME;
    }

    private void setFiles(String[] fileNames) throws CommandException {
        // validation for file names
        files = new File[fileNames.length];
        for (int i = 0; i < fileNames.length; i++) {
            String fname = fileNames[i];
            File f = new File(fname);
            if (!f.isFile()) {
                String msg = "Invalid file name: " + fname;
                LOG.severe(msg);
                throw new CommandException(msg);
            }
            files[i] = f;
        }
    }

    public File[] getFiles() {
        return files;
    }

    private String[] columnNames;
    private String[] columnTypes;
    private boolean hasColumnHeader = false;
    private String timeColumn;
    private long timeValue = -1;
    private String outputDirName;
    private int splitSize;

    private void setOptions(Properties props) throws CommandException {
        // format
        String format = props.getProperty(Config.BI_PREPARE_PARTS_FORMAT);
        if (format == null) {
            throw new CommandException("Format not specified: "
                    + Config.BI_PREPARE_PARTS_FORMAT);
        } else {
            if (!format.equals("csv")) {
                throw new CommandException("Invalid format: "
                        + Config.BI_PREPARE_PARTS_FORMAT);
            }
        }

        boolean setColumns = false;

        // columns
        String columns = props.getProperty(Config.BI_PREPARE_PARTS_COLUMNS);
        if (columns != null) {
            setColumns = true;
            columnNames = columns.split(",");
        }

        // column header
        String columnHeader = props
                .getProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER);
        if (columnHeader == null || !columnHeader.equals("true")) {
            if (!setColumns) {
                throw new CommandException("Column names not set");
            }
        } else {
            hasColumnHeader = true;
        }

        // column types
        String columntypes = props
                .getProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES);
        if (columntypes == null) {
            throw new CommandException("Column types not specified: "
                    + Config.BI_PREPARE_PARTS_COLUMNTYPES);
        } else {
            columnTypes = columntypes.split(",");
        }

        // output dir
        outputDirName = props.getProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR,
                Config.BI_PREPARE_PARTS_OUTPUTDIR_DEFAULTVALUE);

        // time column
        timeColumn = props.getProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN,
                Config.BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE);

        // time column value
        timeValue = Long.parseLong(props
                .getProperty(Config.BI_PREPARE_PARTS_TIMEVALUE));

        // split size
        String splitsize = props.getProperty(
                Config.BI_PREPARE_PARTS_SPLIT_SIZE,
                Config.BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE);
        splitSize = Integer.parseInt(splitsize);
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public String[] getColumnTypes() {
        return columnTypes;
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public boolean hasColumnHeader() {
        return hasColumnHeader;
    }

    public String getOutputDirName() {
        return outputDirName;
    }
    public long getTimeValue() {
        return timeValue;
    }
}
