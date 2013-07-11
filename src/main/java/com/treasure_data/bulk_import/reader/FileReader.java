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
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import com.treasure_data.bulk_import.model.ColumnType;
import com.treasure_data.bulk_import.model.ColumnValue;
import com.treasure_data.bulk_import.model.Row;
import com.treasure_data.bulk_import.model.TimeColumnValue;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;
import com.treasure_data.bulk_import.prepare_parts.Task;
import com.treasure_data.bulk_import.writer.FileWriter;

public abstract class FileReader<T extends PrepareConfiguration> implements Closeable {
    private static final Logger LOG = Logger.getLogger(FileReader.class.getName());

    protected T conf;
    protected FileWriter writer;
    protected Row convertedRow;

    protected String[] columnNames;
    protected ColumnType[] columnTypes;
    protected Set<String> skipColumns = new HashSet<String>();
    protected TimeColumnValue timeColumnValue;

    protected long lineNum = 0;

    private PrintWriter errWriter = null;

    protected FileReader(T conf, FileWriter writer) {
        this.conf = conf;
        this.writer = writer;
    }

    public void configure(Task task) throws PreparePartsException {
        columnNames = conf.getColumnNames();
        columnTypes = conf.getColumnTypes();

        // check compression type of the file
        conf.checkCompressionType(task.fileName);
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public ColumnType[] getColumnTypes() {
        return columnTypes;
    }

    public Set<String> getSkipColumns() {
        return skipColumns;
    }

    public TimeColumnValue getTimeColumnValue() {
        return timeColumnValue;
    }

    public void initializeConvertedRow() {
        ColumnValue[] values = new ColumnValue[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            values[i] = columnTypes[i].createColumnValue();
        }
        convertedRow = new Row(values);
    }

    public void setSkipColumns() {
        String[] excludeColumns = conf.getExcludeColumns();
        String[] onlyColumns = conf.getOnlyColumns();
        for (int i = 0; i < columnNames.length; i++) {
            // check exclude columns
            boolean isExcluded = false;
            for (String excludeColumn : excludeColumns) {
                if (columnNames[i].equals(excludeColumn)) {
                    isExcluded = true;
                    break;
                }
            }

            if (isExcluded) {
                skipColumns.add(columnNames[i]);
                continue;
            }

            // check only columns
            if (onlyColumns.length == 0) {
                continue;
            }

            boolean isOnly = false;
            for (String onlyColumn : onlyColumns) {
                if (columnNames[i].equals(onlyColumn)) {
                    isOnly = true;
                    break;
                }
            }

            if (!isOnly) {
                skipColumns.add(columnNames[i]);
                continue; // not needed though,..
            }
        }
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

    public boolean next() throws PreparePartsException {
        incrementLineNum();
        try {
            // if reader got EOF, it returns false.
            if (!readRow()) {
                return false;
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

    public abstract boolean readRow() throws IOException, PreparePartsException;

    public abstract void convertTypesOfColumns() throws PreparePartsException;

    // Closeable#close()
    public void close() throws IOException {
        if (errWriter != null) {
            errWriter.close();
        }
    }
}
