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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import com.treasure_data.commands.CommandException;

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
                    cproc = new NotNull();
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

    private ICsvMapReader mapReader;
    private String[] header;
    private CellProcessor[] cprocessors;

    public CSVFileReader(Properties props, String fileName)
            throws CommandException {
        initReader(props, fileName);
    }

    @Override
    public void initReader(Properties props, String fileName)
            throws CommandException {
        // TODO refine
        try {
            mapReader = new CsvMapReader(new java.io.FileReader(fileName),
                    CsvPreference.STANDARD_PREFERENCE);
        } catch (IOException e) {
            throw new CommandException(e);
        }

        try {
            // the header columns are used as the keys to the Map
            header = mapReader.getHeader(true);
        } catch (IOException e) {
            throw new CommandException(e);
        }

        // "time,name,price"
        String columnNameList = props
                .getProperty("td.bulk_import.prepare_parts.columns");
        String[] columnNames = columnNameList.split(","); // TODO
        // "long,string,long"
        String columnTypeList = props
                .getProperty("td.bulk_import.prepare_parts.columntypes");
        String[] columnTypes = columnTypeList.split(","); // TODO

        cprocessors = new CellProcessorGen(columnNames, columnTypes).gen();
    }

    public Map<String, Object> readRecord() throws CommandException {
        try {
            Map<String, Object> record = mapReader.read(header, cprocessors);
            LOG.finer(String.format("lineNo=%s, rowNo=%s, customerMap=%s",
                    mapReader.getLineNumber(), mapReader.getRowNumber(), record));
            // TODO debug
            System.out.println(String.format("lineNo=%s, rowNo=%s, customerMap=%s",
                    mapReader.getLineNumber(), mapReader.getRowNumber(), record));
            return record;
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    public void close() throws CommandException {
        if (mapReader != null) {
            try {
                mapReader.close();
            } catch (IOException e) {
                throw new CommandException(e);
            }
        }
    }
}