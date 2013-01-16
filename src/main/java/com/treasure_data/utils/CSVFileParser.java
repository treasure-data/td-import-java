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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public class CSVFileParser extends FileParser {
    private static final Logger LOG = Logger.getLogger(CSVFileParser.class
            .getName());

    private static class CellProcessorGen {
        public CellProcessor[] gen(String[] columnTypes)
                throws CommandException {
            int len = columnTypes.length;
            List<CellProcessor> cprocs = new ArrayList<CellProcessor>(len);
            for (int i = 0; i < len; i++) {
                CellProcessor cproc;
                String type = columnTypes[i];
                if (type.equals("string")) {
                    cproc = new Optional();
                } else if (type.equals("int")) {
                    cproc = new ParseInt();
                } else if (type.equals("long")) {
                    cproc = new ParseLong();
                    // TODO any more...
                } else {
                    throw new CommandException("Unsupported type: " + type);
                }
                cprocs.add(cproc);
            }
            return cprocs.toArray(new CellProcessor[0]);
        }
    }

    private ICsvListReader reader;
    private int timeIndex = -1;
    private long timeValue = -1;
    private String[] columnNames;
    private final String timeColumnName = "time";

    private String[] columnTypes;
    private CellProcessor[] cprocessors;

    public CSVFileParser(PreparePartsRequest request, File file)
            throws CommandException {
        try {
            initReader(request, new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new CommandException(e);
        }
    }

    public CSVFileParser(PreparePartsRequest request, InputStream in)
            throws CommandException {
        initReader(request, in);
    }

    @Override
    public void initReader(PreparePartsRequest request, InputStream in)
            throws CommandException {
        // create reader
        reader = new CsvListReader(new InputStreamReader(in),
                CsvPreference.STANDARD_PREFERENCE);

        // column name e.g. "time,name,price"
        if (request.hasColumnHeader()) {
            try {
                List<String> columnList = reader.read();
                columnNames = columnList.toArray(new String[0]);
            } catch (IOException e) {
                throw new CommandException(e);
            }
        } else {
            columnNames = request.getColumnNames();
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

        cprocessors = new CellProcessorGen().gen(columnTypes);
    }

    public boolean parseRow(MsgpackGZIPFileWriter w) throws CommandException {
        List<Object> row = null;
        try {
            row = reader.read(cprocessors);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.severe("Skip row number: " + getRowNum());
            return true;
        }

        // increment row number
        incrRowNum();

        if (row == null || row.isEmpty()) {
            return false;
        }

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("lineNo=%s, rowNo=%s, customerList=%s",
                    reader.getLineNumber(), reader.getRowNumber(),
                    row));
        }
//        // TODO debug
//        System.out.println(String.format("lineNo=%s, rowNo=%s, customerList=%s",
//                reader.getLineNumber(), reader.getRowNumber(), row));

        try {
            int size = row.size();

            if (size == timeIndex) {
                w.writeBeginRow(size + 1);
            } else {
                w.writeBeginRow(size);
            }

            for (int i = 0; i < size; i++) {
                if (i == timeIndex) {
                    w.write(timeColumnName);
                    w.write(row.get(i));
                } else {
                    w.write(columnNames[i]);
                    w.write(row.get(i));
                }
            }

            if (size == timeIndex) {
                w.write(timeColumnName);
                w.write(timeValue);
            }

            w.writeEndRow();
            return true;
        } catch (Exception e) {
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