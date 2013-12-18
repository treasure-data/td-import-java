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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.treasure_data.td_import.Configuration;
import com.treasure_data.td_import.model.AliasTimeColumnValue;
import com.treasure_data.td_import.model.ColumnType;
import com.treasure_data.td_import.model.TimeColumnSampling;
import com.treasure_data.td_import.model.TimeColumnValue;
import com.treasure_data.td_import.model.TimeValueTimeColumnValue;
import com.treasure_data.td_import.prepare.HHmmssStrftime;
import com.treasure_data.td_import.prepare.PreparePartsException;
import com.treasure_data.td_import.prepare.RegexPrepareConfiguration;
import com.treasure_data.td_import.prepare.Task;
import com.treasure_data.td_import.writer.FileWriter;
import com.treasure_data.td_import.writer.JSONFileWriter;

public class RegexFileReader<T extends RegexPrepareConfiguration> extends FixnumColumnsFileReader<T> {
    private static final Logger LOG = Logger.getLogger(RegexFileReader.class.getName());

    protected BufferedReader reader;
    protected Pattern pat;

    protected String line;
    protected List<String> row = new ArrayList<String>();

    public RegexFileReader(T conf, FileWriter writer)
            throws PreparePartsException {
        super(conf, writer);
    }

    @Override
    public void configure(Task task) throws PreparePartsException {
        super.configure(task);

        sample(task);

        try {
            reader = new BufferedReader(new InputStreamReader(
                    task.createInputStream(conf.getCompressionType()),
                    conf.getCharsetDecoder()));
        } catch (IOException e) {
            throw new PreparePartsException(e);
        }

        pat = Pattern.compile(conf.getRegexPattern());
    }

    public void sample(Task task) throws PreparePartsException {
        BufferedReader sampleReader = null;

        try {
            sampleReader = new BufferedReader(new InputStreamReader(
                    task.createInputStream(conf.getCompressionType()),
                    conf.getCharsetDecoder()));

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

            // if 'time' and the alias columns or 'primary-key' column don't exist, ...
            validateTimeAndPrimaryColumn(timeColumnIndex, aliasTimeColumnIndex);

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

                String sampleLine = sampleReader.readLine();

                if (sampleLine == null) {
                    break;
                }

                Pattern samplePat = Pattern.compile(conf.getRegexPattern());
                Matcher sampleMat = samplePat.matcher(sampleLine);

                if (!sampleMat.matches()) {
                    throw new PreparePartsException("Don't match");
                }

                for (int j = 1; j < (columnNames.length + 1); j++) { // extract groups
                    sampleRow.add(sampleMat.group(j));
                }

                if (isFirstRow) {
                    firstRow.addAll(sampleRow);
                    isFirstRow = false;
                }

                validateRowSize(sampleColumnValues, sampleRow, i);

                // sampling
                for (int j = 0; j < sampleColumnValues.length; j++) {
                    sampleColumnValues[j].parse(sampleRow.get(j));
                }

                sampleRow.clear();
            }

            // initialize types of all columns
            initializeColumnTypes(sampleColumnValues);

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
                } else {
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
            throw new PreparePartsException(e);
        } finally {
            if (sampleReader != null) {
                try {
                    sampleReader.close();
                } catch (IOException e) {
                    throw new PreparePartsException(e);
                }
            }
        }
    }

    public void validateRowSize(TimeColumnSampling[] sampleColumnValues,
            List<String> row, int lineNum) throws PreparePartsException {
        if (sampleColumnValues.length != row.size()) {
            throw new PreparePartsException(
                    String.format("The number of columns to be processed (%d) must " +
                                  "match the number of column types (%d): check that the " +
                                  "number of column types you have defined matches the " +
                                  "expected number of columns being read/written [line: %d] %s",
                            row.size(), columnTypes.length, lineNum, row));
        }
    }

    @Override
    public boolean readRow() throws IOException, PreparePartsException {
        row.clear();

        if ((line = reader.readLine()) == null) {
            return false;
        }

        incrementLineNum();

        Matcher mat = pat.matcher(line);

        if (!mat.matches()) {
            writer.incrementErrorRowNum();
            throw new PreparePartsException(String.format(
                    "line is not matched at apache common log format [line: %d]",
                    getLineNum()));
        }

        // extract groups
        for (int i = 1; i < (columnNames.length + 1); i++) {
            row.add(mat.group(i));
        }

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
        return line;
    }

    @Override
    public void close() throws IOException {
        super.close();

        if (reader != null) {
            reader.close();
        }
    }
}
