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

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.Tokenizer;
import org.supercsv.prefs.CsvPreference;

import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.Row;
import com.treasure_data.bulk_import.ColumnType;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;
import com.treasure_data.bulk_import.prepare_parts.PrepareProcessor;
import com.treasure_data.bulk_import.prepare_parts.proc.ColumnSamplingProc;
import com.treasure_data.bulk_import.prepare_parts.proc.ColumnProc;
import com.treasure_data.bulk_import.prepare_parts.proc.ColumnProcGenerator;
import com.treasure_data.bulk_import.prepare_parts.proc.SkipColumnProc;
import com.treasure_data.bulk_import.writer.FileWriter;
import com.treasure_data.bulk_import.writer.JSONFileWriter;

public class CSVFileReader extends FileReader {
    private static final Logger LOG = Logger.getLogger(CSVFileReader.class.getName());

    protected CsvPreference csvPref;
    private Tokenizer reader;
    private Row.TimeColumnValue timeColumnValue = null;

    public CSVFileReader(PrepareConfiguration conf, FileWriter writer) throws PreparePartsException {
        super(conf, writer);
    }

    @Override
    public void configure(PrepareProcessor.Task task) throws PreparePartsException {
        super.configure(task);

        // initialize csv preference
        csvPref = new CsvPreference.Builder(conf.getQuoteChar(),
                conf.getDelimiterChar(), conf.getNewline().newline()).build();

        // if conf object doesn't have column names, types, etc,
        // sample method checks those values.
        sample(task);

        try {
            reader = new Tokenizer(new InputStreamReader(
                    task.createInputStream(conf.getCompressionType()),
                    conf.getCharsetDecoder()), csvPref);
            if (conf.hasColumnHeader()) {
                // header line is skipped
                reader.readColumns(new ArrayList<String>());
                incrementLineNum();
            }
        } catch (IOException e) {
            throw new PreparePartsException(e);
        }
    }

    private void sample(PrepareProcessor.Task task) throws PreparePartsException {
        // create sample reader
        CsvListReader sampleReader = null;
        try {
            // TODO sample reader is not closed
            sampleReader = new CsvListReader(new InputStreamReader(
                    task.createInputStream(conf.getCompressionType()), conf.getCharsetDecoder()), csvPref);
        } catch (IOException e) {
            throw new PreparePartsException(e);
        }

        try {
            int timeColumnIndex = -1;
            int aliasTimeColumnIndex = -1;

            // extract column names
            // e.g. 
            // 1) [ "time", "name", "price" ]
            // 2) [ "timestamp", "name", "price" ]
            // 3) [ "name", "price" ]
            if (conf.hasColumnHeader()) {
                List<String> columnList = sampleReader.read();
                if (columnNames == null || columnNames.length == 0) {
                    columnNames = columnList.toArray(new String[0]);
                    conf.setColumnNames(columnNames);
                }
            }

            // get index of 'time' column
            // [ "time", "name", "price" ] as all columns is given,
            // the index is zero.
            for (int i = 0; i < columnNames.length; i++) {
                if (columnNames[i].equals(
                        Configuration.BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE)) {
                    timeColumnIndex = i;
                    break;
                }
            }

            // get index of specified alias time column
            // [ "timestamp", "name", "price" ] as all columns and
            // "timestamp" as alias time column are given, the index is zero.
            //
            // if 'time' column exists in row data, the specified alias
            // time column is ignore.
            if (timeColumnIndex < 0 && conf.getAliasTimeColumn() != null) {
                for (int i = 0; i < columnNames.length; i++) {
                    if (columnNames[i].equals(conf.getAliasTimeColumn())) {
                        aliasTimeColumnIndex = i;
                        break;
                    }
                }
            }

            // if 'time' and the alias columns don't exist, ...
            if (timeColumnIndex < 0 && aliasTimeColumnIndex < 0) {
                if (conf.getTimeValue() >= 0) {
                } else {
                    throw new PreparePartsException(
                            "Time column not found. --time-column or --time-value option is required");
                }
            }

            // read sample rows
            List<String> firstRow = new ArrayList<String>();
            final int sampleRowSize = conf.getSampleRowSize();
            CellProcessor[] sampleProcs = ColumnProcGenerator.generateSampleCellProcessors(
                    columnNames, sampleRowSize);
            boolean isFirstRow = false;
            for (int i = 0; i < sampleRowSize; i++) {
                List<Object> row = sampleReader.read(sampleProcs);
                if (!isFirstRow) {
                    for (Object c : row) {
                        if (c != null) {
                            firstRow.add(c.toString());
                        } else {
                            firstRow.add(null);
                        }
                    }
                    isFirstRow = true;
                }

                if (row == null || row.isEmpty()) {
                    break;
                }
            }

            // initialize types of all columns
            if (columnTypes == null || columnTypes.length == 0) {
                columnTypes = new ColumnType[columnNames.length];
                for (int i = 0; i < columnTypes.length; i++) {
                    columnTypes[i] = ColumnSamplingProc.getColumnType(sampleProcs[i]);
                }
                conf.setColumnTypes(columnTypes);
            }

            // initialize time column value
            if (timeColumnIndex >= 0) {
                timeColumnValue = new Row.TimeColumnValue(timeColumnIndex,
                        columnTypes[timeColumnIndex], conf.getTimeFormat());
            } else if (aliasTimeColumnIndex >= 0) {
                timeColumnValue = new Row.AliasTimeColumnValue(
                        aliasTimeColumnIndex,
                        columnTypes[aliasTimeColumnIndex], conf.getTimeFormat());
            } else {
                timeColumnValue = new Row.TimeValueTimeColumnValue(
                        conf.getTimeValue());
            }

            initializeConvertedRow(timeColumnValue instanceof Row.TimeColumnValue, timeColumnValue);

            // print first sample row
            JSONFileWriter w = new JSONFileWriter(conf);
            w.setColumnNames(getColumnNames());
            w.setColumnTypes(getColumnTypes());

//            // add attributes of exclude/only columns to column types
//            addExcludeAndOnlyColumnsFilter(onelineProcs);

            try {
                // convert each column in row
                convertTypesOfColumns();
                // write each column value
                w.next(convertedRow);
                String ret = w.toJSONString();
                if (ret != null) {
                    LOG.info("sample row: " + ret);
                } else  {
                    LOG.info("cannot get sample row");
                }
            } finally {
                if (w != null) {
                    w.closeSilently();
                }
            }
        } catch (IOException e) {
            throw new PreparePartsException(e);
        }
    }

    @Override
    public boolean next() throws PreparePartsException {
        incrementLineNum();
        try {
            // if reader got EOF, it returns false.
            if (!reader.readColumns(rawRow)) {
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

    void convertTypesOfColumns() {
        for (int i = 0; i < rawRow.size(); i++) {
            Row.ColumnValue v = convertedRow.getValue(i);
            columnTypes[i].convertTypeInto(rawRow.get(i), v);
            convertedRow.setValue(i, v);
        }
    }

    public void close() throws PreparePartsException {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new PreparePartsException(e);
            }
        }
    }

    void addExcludeAndOnlyColumnsFilter(CellProcessor[] cellProcs) {
        String[] excludeColumns = conf.getExcludeColumns();
        String[] onlyColumns = conf.getOnlyColumns();
        for (int i = 0; i < cellProcs.length; i++) {
            ColumnProc colProc = (ColumnProc) cellProcs[i];
            String cname = colProc.getColumnName();

            // check exclude columns
            boolean isExcluded = false;
            for (String excludeColumn : excludeColumns) {
                if (cname.equals(excludeColumn)) {
                    isExcluded = true;
                    break;
                }
            }

            if (isExcluded) {
                cellProcs[i] = new SkipColumnProc(colProc);
                continue;
            }

            // check only columns
            if (onlyColumns.length == 0) {
                continue;
            }

            boolean isOnly = false;
            for (String onlyColumn : onlyColumns) {
                if (cname.equals(onlyColumn)) {
                    isOnly = true;
                    break;
                }
            }

            if (!isOnly) {
                cellProcs[i] = new SkipColumnProc(colProc);
                continue; // not needed though,..
            }
        }
    }

}