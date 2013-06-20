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
package com.treasure_data.bulk_import.reader;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.treasure_data.bulk_import.Row;
import com.treasure_data.bulk_import.ColumnType;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;
import com.treasure_data.bulk_import.prepare_parts.PrepareProcessor;
import com.treasure_data.bulk_import.writer.FileWriter;

public abstract class FileReader {
    private static final Logger LOG = Logger.getLogger(FileReader.class.getName());

    protected PrepareConfiguration conf;
    protected FileWriter writer;

    protected List<String> rawRow = new ArrayList<String>();
    protected Row convertedRow;

    protected String[] columnNames;
    protected ColumnType[] columnTypes;

    protected long lineNum = 0;
    protected long rowNum = 0;
    protected PrepareConfiguration.CompressionType compressionType;

    private PrintWriter errWriter = null;

    protected FileReader(PrepareConfiguration conf, FileWriter writer) {
        this.conf = conf;
        this.writer = writer;
    }

    public void configure(PrepareProcessor.Task task) throws PreparePartsException {
        compressionType = conf.checkCompressionType(task.fileName);
        columnNames = conf.getColumnNames();
        columnTypes = conf.getColumnTypes();
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public ColumnType[] getColumnTypes() {
        return columnTypes;
    }

    public void initializeConvertedRow(boolean needAdditionalTimeColumn,
            Row.TimeColumnValue timeColumnValue) {
        Row.ColumnValue[] values = new Row.ColumnValue[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            values[i] = columnTypes[i].createColumnValue();
        }
        convertedRow = new Row(values, needAdditionalTimeColumn, timeColumnValue);
    }

    public void resetLineNum() {
        lineNum = 0;
    }

    public void incrementLineNum() {
        lineNum++;
    }

    public long getLineNum() {
        return lineNum;
    }

    public void resetRowNum() {
        rowNum = 0;
    }

    public void incrementRowNum() {
        rowNum++;
    }

    public long getRowNum() {
        return rowNum;
    }

    public void setErrorRecordWriter(OutputStream errStream) {
        if (errStream != null) {
            errWriter = new PrintWriter(errStream);
        }
    }

    public void writeErrorRecord(String msg) {
        if (errWriter != null) {
            errWriter.println(msg);
        }
    }

    public void closeErrorRecordWriter() {
        if (errWriter != null) {
            errWriter.close();
        }
    }

    public abstract boolean next() throws PreparePartsException;

    public abstract void close() throws PreparePartsException;

    public void closeSilently() {
        try {
            close();
            closeErrorRecordWriter();
        } catch (PreparePartsException e) {
            LOG.severe(e.getMessage());
        }
    }
}
