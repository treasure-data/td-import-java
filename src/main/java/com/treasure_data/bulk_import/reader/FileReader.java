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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.treasure_data.bulk_import.model.ColumnType;
import com.treasure_data.bulk_import.model.ColumnValue;
import com.treasure_data.bulk_import.model.Row;
import com.treasure_data.bulk_import.model.TimeColumnValue;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;
import com.treasure_data.bulk_import.prepare_parts.PrepareProcessor;
import com.treasure_data.bulk_import.writer.FileWriter;

public abstract class FileReader implements Closeable {
    private static final Logger LOG = Logger.getLogger(FileReader.class.getName());

    protected PrepareConfiguration conf;
    protected FileWriter writer;

    protected List<String> rawRow = new ArrayList<String>();
    protected Row convertedRow;

    protected String[] columnNames;
    protected ColumnType[] columnTypes;
    protected Set<Integer> skipColumns = new HashSet<Integer>();

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

    public Set<Integer> getSkipColumns() {
        return skipColumns;
    }

    public void initializeConvertedRow(TimeColumnValue timeColumnValue) {
        ColumnValue[] values = new ColumnValue[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            values[i] = columnTypes[i].createColumnValue();
        }
        convertedRow = new Row(values, timeColumnValue);
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

    public abstract boolean readRow() throws IOException;

    public boolean next() throws PreparePartsException {
        incrementLineNum();
        try {
            // if reader got EOF, it returns false.
            if (!readRow()) {
                return false;
            }

            incrementRowNum();

            int rawRowSize = rawRow.size();
            if (rawRowSize != columnTypes.length) {
                throw new PreparePartsException(String.format(
                        "The number of columns to be processed (%d) must " +
                        "match the number of column types (%d): check that the " +
                        "number of column types you have defined matches the " +
                        "expected number of columns being read/written [line: %d]",
                        rawRowSize, columnTypes.length, getLineNum()));
            }

            // convert each column in row
            convertTypesOfColumns();

            // write each column value
            writer.next(convertedRow);
            writer.incrementRowNum();
        } catch (IOException e) {
            // if reader throw I/O error, parseRow throws PreparePartsException.
            LOG.throwing("CSVFileParser", "parseRow", e);
            throw new PreparePartsException(e);
        } catch (PreparePartsException e) {
            // TODO the row data should be written to error rows file
            LOG.warning(e.getMessage());
        }
        return true;
    }

    public void convertTypesOfColumns() {
        for (int i = 0; i < rawRow.size(); i++) {
            ColumnValue v = convertedRow.getValue(i);
            columnTypes[i].convertTypeInto(rawRow.get(i), v);
            convertedRow.setValue(i, v);
        }
    }

    // Closeable#close()
    public void close() throws IOException {
        if (errWriter != null) {
            errWriter.close();
        }
    }
}
