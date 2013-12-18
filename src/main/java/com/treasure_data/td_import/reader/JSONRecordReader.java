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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.treasure_data.td_import.model.ColumnType;
import com.treasure_data.td_import.model.ColumnValue;
import com.treasure_data.td_import.model.Row;
import com.treasure_data.td_import.prepare.JSONPrepareConfiguration;
import com.treasure_data.td_import.prepare.PreparePartsException;
import com.treasure_data.td_import.prepare.Task;
import com.treasure_data.td_import.writer.RecordWriter;
import com.treasure_data.td_import.writer.JSONRecordWriter;

public class JSONRecordReader extends DynamicColumnsFileReader<JSONPrepareConfiguration> {
    private static final Logger LOG = Logger.getLogger(JSONRecordReader.class.getName());

    protected BufferedReader reader;
    protected JSONParser parser;

    protected String line;
    protected Map<String, Object> row;

    public JSONRecordReader(JSONPrepareConfiguration conf, RecordWriter writer) {
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

        // create parser
        parser = new JSONParser();
    }

    public void sample(Task task) throws PreparePartsException {
        BufferedReader sampleReader = null;
        try {
            sampleReader = new BufferedReader(new InputStreamReader(
                    task.createInputStream(conf.getCompressionType()),
                    conf.getCharsetDecoder()));

            // read first line only
            line = sampleReader.readLine();
            if (line == null) {
                String msg = String.format("Anything is not read or EOF [line: 1] %s", task.getSource());
                LOG.severe(msg);
                throw new PreparePartsException(msg);
            }

            try {
                JSONParser sampleParser = new JSONParser();
                row = (Map<String, Object>) sampleParser.parse(line);
                if (row == null) {
                    String msg = String.format("Anything is not parsed [line: 1] %s", task.getSource());
                    LOG.severe(msg);
                    throw new PreparePartsException(msg);
                }
            } catch (ParseException e) {
                LOG.log(Level.SEVERE, String.format("Anything is not parsed [line: 1] %s", task.getSource()), e);
                throw new PreparePartsException(e);
            }

            // print first sample row
            JSONRecordWriter w = null;
            try {
                w = new JSONRecordWriter(conf);
                setColumnNames();
                w.setColumnNames(getColumnNames());
                setColumnTypes();
                w.setColumnTypes(getColumnTypes());
                setSkipColumns();
                w.setSkipColumns(getSkipColumns());
                setTimeColumnValue();
                w.setTimeColumnValue(getTimeColumnValue());

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
            LOG.log(Level.SEVERE, "during sample method execution", e);
            throw new PreparePartsException(e);
        } finally {
            if (sampleReader != null) {
                try {
                    sampleReader.close();
                } catch (IOException e) {
                    LOG.log(Level.SEVERE, "sampling reader cannot be closed", e);
                    throw new PreparePartsException(e);
                }
            }
        }
    }

    @Override
    public void setColumnNames() {
        columnNames = row.keySet().toArray(new String[0]);
    }

    @Override
    public void setColumnTypes() {
        columnTypes = new ColumnType[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            Object v = row.get(columnNames[i]);
            columnTypes[i] = toColumnType(v);
        }
    }

    @Override
    public void setSkipColumns() {
        super.setSkipColumns();
    }

    @Override
    public boolean readRow() throws IOException {
        try {
            line = reader.readLine();
            if (line == null) {
                return false;
            }

            incrementLineNum();

            row = (Map<String, Object>) parser.parse(line);
            return row != null;
        } catch (ParseException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void convertTypesOfColumns() throws PreparePartsException {
        ColumnValue[] columnValues = new ColumnValue[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columnValues[i] = columnTypes[i].createColumnValue();
            columnTypes[i].setColumnValue(row.get(columnNames[i]), columnValues[i]);
        }

        convertedRow = new Row(columnValues);
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
