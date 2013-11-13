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
package com.treasure_data.td_import.reader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.supercsv.io.Tokenizer;
import org.supercsv.prefs.CsvPreference;

import com.treasure_data.td_import.model.ColumnType;
import com.treasure_data.td_import.model.TimeColumnSampling;
import com.treasure_data.td_import.prepare.CSVPrepareConfiguration;
import com.treasure_data.td_import.prepare.PreparePartsException;
import com.treasure_data.td_import.prepare.Task;
import com.treasure_data.td_import.writer.FileWriter;
import com.treasure_data.td_import.writer.JSONFileWriter;

public class CSVFileReader extends FixnumColumnsFileReader<CSVPrepareConfiguration> {
    private static final Logger LOG = Logger.getLogger(CSVFileReader.class.getName());

    protected CsvPreference csvPref;
    private Tokenizer tokenizer;
    protected List<String> row = new ArrayList<String>();

    public CSVFileReader(CSVPrepareConfiguration conf, FileWriter writer)
            throws PreparePartsException {
        super(conf, writer);
    }

    @Override
    public void configure(Task task) throws PreparePartsException {
        super.configure(task);

        // initialize csv preference
        csvPref = new CsvPreference.Builder(conf.getQuoteChar().quote(),
                conf.getDelimiterChar(), conf.getNewline().newline()).build();

        // if conf object doesn't have column names, types, etc,
        // sample method checks those values.
        sample(task);

        try {
            tokenizer = new Tokenizer(new InputStreamReader(
                    task.createInputStream(conf.getCompressionType()),
                    conf.getCharsetDecoder()), csvPref);
            if (conf.hasColumnHeader()) {
                // header line is skipped
                incrementLineNum();
                tokenizer.readColumns(new ArrayList<String>());
            }
        } catch (IOException e) {
            LOG.throwing(this.getClass().getName(), "configure", e);
            throw new PreparePartsException(e);
        }
    }

    private void setColumnNamesWithColumnHeader(Tokenizer tokenizer) throws IOException {
        List<String> sampleRow = new ArrayList<String>();
        if (conf.hasColumnHeader()) {
            tokenizer.readColumns(sampleRow);
            if (columnNames == null || columnNames.length == 0) {
                columnNames = sampleRow.toArray(new String[0]);
                conf.setColumnNames(columnNames);
            }
        }
    }

    public void sample(Task task) throws PreparePartsException {
        Tokenizer sampleTokenizer = null;

        try {
            // create sample reader
            sampleTokenizer = new Tokenizer(new InputStreamReader(
                    task.createInputStream(conf.getCompressionType()),
                    conf.getCharsetDecoder()), csvPref);

            // extract column names
            // e.g. 
            // 1) [ "time", "name", "price" ]
            // 2) [ "timestamp", "name", "price" ]
            // 3) [ "name", "price" ]
            setColumnNamesWithColumnHeader(sampleTokenizer);

            // get index of 'time' column
            // [ "time", "name", "price" ] as all columns is given,
            // the index is zero.
            int timeColumnIndex = getTimeColumnIndex();

            // get index of specified alias time column
            // [ "timestamp", "name", "price" ] as all columns and
            // "timestamp" as alias time column are given, the index is zero.
            //
            // if 'time' column exists in row data, the specified alias
            // time column is ignore.
            int aliasTimeColumnIndex = getAliasTimeColumnIndex(timeColumnIndex);

            // if 'time' and the alias columns don't exist, ...
            if (timeColumnIndex < 0 && aliasTimeColumnIndex < 0) {
                if (conf.getTimeValue() >= 0) {
                } else {
                    throw new PreparePartsException(
                            "Time column not found. --time-column or --time-value option is required");
                }
            }

            boolean isFirstRow = true;
            List<String> firstRow = new ArrayList<String>();
            final int sampleRowSize = conf.getSampleRowSize();
            TimeColumnSampling[] sampleColumnValues = new TimeColumnSampling[columnNames.length];
            for (int i = 0; i < sampleColumnValues.length; i++) {
                sampleColumnValues[i] = new TimeColumnSampling(sampleRowSize);
            }

            // read some rows
            List<String> sampleRow = new ArrayList<String>();
            for (int i = 0; i < sampleRowSize; i++) {
                if (!isFirstRow && (columnTypes == null || columnTypes.length == 0)) {
                    break;
                }

                sampleTokenizer.readColumns(sampleRow);

                if (sampleRow == null || sampleRow.isEmpty()) {
                    break;
                }

                if (isFirstRow) {
                    firstRow.addAll(sampleRow);
                    isFirstRow = false;
                }

                if (sampleColumnValues.length != sampleRow.size()) {
                    throw new PreparePartsException(String.format(
                            "The number of columns to be processed (%d) must " +
                            "match the number of column types (%d): check that the " +
                            "number of column types you have defined matches the " +
                            "expected number of columns being read/written [line: %d] %s",
                            sampleRow.size(), columnTypes.length, i, sampleRow));
                }

                // sampling
                for (int j = 0; j < sampleColumnValues.length; j++) {
                    sampleColumnValues[j].parse(sampleRow.get(j));
                }
            }

            // initialize types of all columns
            initializeColumnTypes(sampleColumnValues);

            // initialize time column value
            setTimeColumnValue(sampleColumnValues, timeColumnIndex, aliasTimeColumnIndex);

            initializeConvertedRow();

            // check properties of exclude/only columns
            setSkipColumns();

            // print first sample row
            JSONFileWriter w = null;
            try {
                w = new JSONFileWriter(conf);
                w.setColumnNames(getColumnNames());
                w.setColumnTypes(getColumnTypes());
                w.setSkipColumns(getSkipColumns());
                w.setTimeColumnValue(getTimeColumnValue());

                this.row.addAll(firstRow);

                // convert each column in row
                convertTypesOfColumns();
                // write each column value
                w.next(convertedRow);
                String ret = w.toJSONString();
                String msg = null;
                if (ret != null) {
                    msg = "sample row: " + ret;
                } else  {
                    msg = "cannot get sample row";
                }
                System.out.println(msg);
                LOG.info(msg);
            } finally {
                if (w != null) {
                    w.close();
                }
            }
        } catch (IOException e) {
            LOG.throwing(this.getClass().getName(), "sample", e);
            throw new PreparePartsException(e);
        } finally {
            if (sampleTokenizer != null) {
                try {
                    sampleTokenizer.close();
                } catch (IOException e) {
                    LOG.throwing(this.getClass().getName(), "sample", e);
                    throw new PreparePartsException(e);
                }
            }
        }
    }

    @Override
    public boolean readRow() throws IOException, PreparePartsException {
        row.clear();
        if (!tokenizer.readColumns(row)) {
            return false;
        }

        incrementLineNum();

        int rawRowSize = row.size();
        if (rawRowSize != columnTypes.length) {
            writer.incrementErrorRowNum();
            throw new PreparePartsException(String.format(
                    "The number of columns to be processed (%d) must " +
                    "match the number of column types (%d): check that the " +
                    "number of column types you have defined matches the " +
                    "expected number of columns being read/written [line: %d]",
                    rawRowSize, columnTypes.length, getLineNum()));
        }

        return true;
    }

    @Override
    public void convertTypesOfColumns() throws PreparePartsException {
        for (int i = 0; i < this.row.size(); i++) {
            columnTypes[i].convertType(this.row.get(i), convertedRow.getValue(i));
        }
    }

    @Override
    public String getCurrentRow() {
        return row.toString();
    }

    @Override
    public void close() throws IOException {
        super.close();

        if (tokenizer != null) {
            tokenizer.close();
        }
    }

}