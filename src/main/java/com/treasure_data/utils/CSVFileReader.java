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
package com.treasure_data.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public class CSVFileReader extends FileReader {
    private static final Logger LOG = Logger.getLogger(CSVFileReader.class
            .getName());

    private static class CellProcessorGen {
        private String[] columnTypes;

        public CellProcessorGen(String[] columnNames, String[] columnTypes) {
            this.columnTypes = columnTypes;
        }

        public CellProcessor[] gen() throws CommandException {
            int len = columnTypes.length;
            List<CellProcessor> cprocs = new ArrayList<CellProcessor>(len);
            for (int i = 0; i < len; i++) {
                CellProcessor cproc;
                String type = columnTypes[i];
                if (type.equals("string")) {
                    //cproc = new NotNull();
                    cproc = new Optional();
                } else if (type.equals("int")) {
                    cproc = new ParseInt();
                } else if (type.equals("long")) {
                    cproc = new ParseLong();
                    // TODO any more...
                    // TODO any more...
                    // TODO any more...
                } else {
                    throw new CommandException("Not such type: " + type);
                }
                cprocs.add(cproc);
            }
            return cprocs.toArray(new CellProcessor[0]);
        }
    }

    private static interface CellProcessor2 {
        public Value doIt(String text);
    }

    private static class StringProc implements CellProcessor2 {
        public Value doIt(String text) {
            return null; // TODO
        }
    }

    private static class IntProc implements CellProcessor2 {
        public Value doIt(String text) {
            return null; // TODO
        }
    }

    private static class LongProc implements CellProcessor2 {
        public Value doIt(String text) {
            return null; // TODO
        }
    }

    private static class CellProcessorGen2 {
        private String[] columnTypes;

        public CellProcessorGen2(String[] columnTypes) {
            this.columnTypes = columnTypes;
        }

        public CellProcessor2[] gen() throws CommandException {
            int len = columnTypes.length;
            List<CellProcessor2> cprocs = new ArrayList<CellProcessor2>(len);
            for (int i = 0; i < len; i++) {
                CellProcessor2 cproc;
                String type = columnTypes[i];
                if (type.equals("string")) {
                    cproc = new StringProc();
                } else if (type.equals("int")) {
                    cproc = new IntProc();
                } else if (type.equals("long")) {
                    cproc = new LongProc();
                    // TODO any more...
                    // TODO any more...
                    // TODO any more...
                } else {
                    throw new CommandException("Not such type: " + type);
                }
                cprocs.add(cproc);
            }
            return cprocs.toArray(new CellProcessor2[0]);
        }
    }

    //private CsvListReader listReader;
    private BufferedReader reader;
    private int timeIndex = -1;
    private long timeValue = -1;
    private String[] columnNames;
    private Value[] columnNameValues;
    private Value timeColumnValue = ValueFactory.createRawValue("time");
    private String[] columnTypes;
    private CellProcessor2[] cprocessors;

    public CSVFileReader(PreparePartsRequest request, File file)
            throws CommandException {
        try {
            initReader(request, new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new CommandException(e);
        }
    }

    public CSVFileReader(PreparePartsRequest request, InputStream in)
            throws CommandException {
        initReader(request, in);
    }

    @Override
    public void initReader(PreparePartsRequest request, InputStream in) throws CommandException {
//        listReader = new CsvListReader(new InputStreamReader(in),
//                CsvPreference.STANDARD_PREFERENCE);

        reader = new BufferedReader(new InputStreamReader(in));

        // "time,name,price"
        try {
            if (request.hasColumnHeader()) {
                // the header columns are used as the keys to the Map
                String line = reader.readLine();
                columnNames = line.split(",");
                //columnNames = listReader.getHeader(true);
            } else {
                columnNames = request.getColumnNames();
            }
        } catch (IOException e) {
            throw new CommandException(e);
        }

        // column name values
        columnNameValues = new Value[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columnNameValues[i] = ValueFactory.createRawValue(columnNames[i]);
        }

        String timeColumn = request.getTimeColumn();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equals(timeColumn)) {
                timeIndex = i;
                break;
            }
        }
        if (timeIndex < 0) {
            timeValue = request.getTimeValue();
            if (timeValue < 0) {
                throw new CommandException(
                        "Time column not found. --time-column or --time-value option is required");
            } else {
                timeIndex = columnNames.length;
            }
        }

        // "long,string,long"
        columnTypes = request.getColumnTypes();

        cprocessors = new CellProcessorGen2(columnTypes).gen();
    }

    public Value[] readRecord() throws CommandException {
        try {
            String line = reader.readLine();

//            List<Object> record = listReader.read(cprocessors);
//            LOG.finer(String.format("lineNo=%s, rowNo=%s, customerList=%s",
//                    listReader.getLineNumber(), listReader.getRowNumber(), record));
//            // TODO debug
//            System.out.println(String.format("lineNo=%s, rowNo=%s, customerList=%s",
//                    listReader.getLineNumber(), listReader.getRowNumber(), record));

            if (line == null || line.isEmpty()) {
                return null;
            }

            String[] columnValues = line.split(",");
            int size = columnValues.length;

            Value[] kvs;
            if (size == timeIndex) {
                kvs = new Value[2 * (size + 1)];
            } else {
                kvs = new Value[2 * size];
            }

            for (int i = 0; i < size; i++) {
                if (i == timeIndex) {
                    kvs[2 * i] = timeColumnValue;
                    kvs[2 * i + 1] = ValueFactory.createIntegerValue(Long.parseLong(columnValues[i]));
                } else {
                    kvs[2 * i] = columnNameValues[i];
                    kvs[2 * i + 1] = ValueFactory.createRawValue((String) columnValues[i]); // TODO
                }
            }
            if (size == timeIndex) {
                kvs[2 * size] = timeColumnValue;
                kvs[2 * size + 1] = ValueFactory.createIntegerValue(timeValue);
            }

            return kvs;
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    public void close() throws CommandException {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new CommandException(e);
            }
        }
    }
}