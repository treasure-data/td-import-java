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
package com.treasure_data.bulk_import.prepare_parts;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.io.Tokenizer;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;
import org.supercsv.util.Util;

import com.treasure_data.bulk_import.Config;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfig;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfig.ColumnType;
import com.treasure_data.bulk_import.prepare_parts.proc.ColumnProcessor;
import com.treasure_data.bulk_import.prepare_parts.proc.ColumnProcessorGenerator;

public class CSVFileParser extends FileParser {
    private static final Logger LOG = Logger.getLogger(CSVFileParser.class.getName());

    private Tokenizer reader;
    private CsvPreference csvPref;
    private CellProcessor[] cprocs;

    private int timeIndex = -1;
    private Long timeValue = new Long(-1);
    private int aliasTimeIndex = -1;
    private String[] allColumnNames;
    private List<Integer> extractedColumnIndexes;
    private ColumnType[] allSuggestedColumnTypes;

    public CSVFileParser(PrepareConfig conf) throws PreparePartsException {
        super(conf);
    }

    @Override
    public void configure(String fileName) throws PreparePartsException {
        super.configure(fileName);

        // CSV preference
        csvPref = new CsvPreference.Builder('"', conf.getDelimiterChar(),
                conf.getNewline().newline()).build();
    }

    @Override
    public void sample(InputStream in) throws PreparePartsException {
        // create sample reader
        CsvListReader sampleReader = new CsvListReader(new InputStreamReader(
                in, decoder), csvPref);

        try {
            // extract all column names
            // e.g. 
            // 1) [ "time", "name", "price" ]
            // 2) [ "timestamp", "name", "price" ]
            // 3) [ "name", "price" ]
            if (conf.hasColumnHeader()) {
                List<String> columnList = sampleReader.read();
                allColumnNames = columnList.toArray(new String[0]);
            } else {
                allColumnNames = conf.getColumnNames();
            }

            // get index of specified alias time column
            // [ "timestamp", "name", "price" ] as all columns and
            // "timestamp" as alias time column are given, the index is zero.
            if (conf.getAliasTimeColumn() != null) {
                for (int i = 0; i < allColumnNames.length; i++) {
                    if (allColumnNames[i].equals(conf.getAliasTimeColumn())) {
                        aliasTimeIndex = i;
                        break;
                    }
                }
            }

            // get index of 'time' column
            // [ "time", "name", "price" ] as all columns is given,
            // the index is zero.
            for (int i = 0; i < allColumnNames.length; i++) {
                if (allColumnNames[i].equals(
                        Config.BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE)) {
                    timeIndex = i;
                    break;
                }
            }

            if (timeIndex < 0) {
                // if 'time' column is not included in all columns
                // e.g. [ "name", "price" ]
                timeValue = conf.getTimeValue();
                if (aliasTimeIndex >= 0 || timeValue > 0) {
                    // 'time' column is appended to all columns (last elem) 
                    timeIndex = allColumnNames.length;
                } else {
                    throw new PreparePartsException(
                            "Time column not found. --time-column or --time-value option is required");
                }
            }

            extractedColumnIndexes = new ArrayList<Integer>();
            String[] onlyColumns = conf.getOnlyColumns();
            String[] excludeColumns = conf.getExcludeColumns();
            for (int i = 0; i < allColumnNames.length; i++) {
                String cname = allColumnNames[i];

                // column is included in exclude-columns?
                if (excludeColumns.length != 0 && !cname.equals("time")) {
                    boolean isExcludeColumn = false;
                    for (int j = 0; j < excludeColumns.length; j++) {
                        if (cname.equals(excludeColumns[j])) {
                            isExcludeColumn = true;
                            break;
                        }
                    }
                    if (isExcludeColumn) {
                        continue;
                    }
                }

                // column is included in only-columns?
                if (onlyColumns.length == 0) {
                    extractedColumnIndexes.add(i);
                    continue;
                } else {
                    boolean isOnlyColumn = false;
                    for (int j = 0; j < onlyColumns.length; j++) {
                        if (cname.equals(onlyColumns[j])) {
                            isOnlyColumn = true;
                            break;
                        }
                    }
                    if (isOnlyColumn) {
                        extractedColumnIndexes.add(i);
                    }
                }
            }

            // check whether time column is included in extracted column
            if (timeValue < 0) {
                if (timeIndex != allColumnNames.length) {
                    boolean hasTimeRepresentedColumn = false;
                    for (Integer i : extractedColumnIndexes) {
                        if (i == timeIndex) {
                            hasTimeRepresentedColumn = true;
                            break;
                        }
                    }
                    if (!hasTimeRepresentedColumn) {
                        throw new PreparePartsException(
                                "'time' represented column is not included in specified columns");
                    }
                }
            }

            // new String[] { "long", "string", "long" }
            String[] columnTypes = conf.getColumnTypes();
            int columnTypeSize = columnTypes.length;
            if (columnTypeSize != 0 && columnTypeSize != allColumnNames.length) {
                throw new PreparePartsException(String.format(
                        "mismatched between size of specified column types (%d) and size of columns (%d)",
                        columnTypeSize, allColumnNames.length));
            }

        // TODO

            allSuggestedColumnTypes = new ColumnType[allColumnNames.length];
            for (int i = 0; i < allColumnNames.length; i++) {
                if (i == timeIndex) {
                    allSuggestedColumnTypes[i] = PrepareConfig.ColumnType.LONG;
                } else if (i == aliasTimeIndex) {
                    allSuggestedColumnTypes[i] = PrepareConfig.ColumnType.LONG;
                } else {
                    allSuggestedColumnTypes[i] = PrepareConfig.ColumnType.STRING;
                }
            }
        } catch (IOException e) {
            throw new PreparePartsException(e);
        } finally {

        }
    }

    @Override
    public void parse(InputStream in) throws PreparePartsException {
        // create reader
        reader = new Tokenizer(new InputStreamReader(in, decoder), csvPref);
        if (conf.hasColumnHeader()) {
            // header line is skipped
            try {
                reader.readColumns(new ArrayList<String>());
                incrLineNum();
            } catch (IOException e) {
                throw new PreparePartsException(e);
            }
        }

        // create cell processors
        ColumnProcessor[] cprocs = new ColumnProcessorGenerator().generate(
                allColumnNames, allSuggestedColumnTypes, writer);
        this.cprocs = new CellProcessor[cprocs.length];
        for (int i = 0; i < cprocs.length; i++) {
            this.cprocs[i] = (CellProcessor) cprocs[i];
        }

        List<String> row = new ArrayList<String>();
        while (parseRow(row)) {
            incrLineNum();
        }
    }

    private boolean parseRow(List<String> row) throws PreparePartsException {
        boolean ret = false;
        try {
            ret = reader.readColumns(row);
        } catch (IOException e) {
            // TODO FIXME more detail

            // catch IOException and SuperCsvCellProcessorException
            e.printStackTrace();

            // TODO and parsent-encoded row?
            String msg = String.format("reason: %s, line: %d",
                    e.getMessage(), getRowNum());
            writeErrorRecord(msg);

            LOG.warning("Skip row number: " + getRowNum());
            return true;
        }

        if (!ret) {
            return false;
        }

        // increment row number
        incrRowNum();

        int allSize = cprocs.length;

        if (allSize == timeIndex) {
            writer.writeBeginRow(extractedColumnIndexes.size() + 1);
        } else {
            writer.writeBeginRow(extractedColumnIndexes.size());
        }

        {
            final CsvContext context = new CsvContext(0, 0, 1); // TODO
            if (row.size() != cprocs.length) {
                throw new SuperCsvException(String.format(
                        "The number of columns to be processed (%d) must match the number of CellProcessors (%d): check that the number"
                                + " of CellProcessors you have defined matches the expected number of columns being read/written",
                                row.size(), cprocs.length), context);
            }

            for (int i = 0; i < row.size(); i++) {
                cprocs[i].execute(row.get(i), context);
            }
        }
        //parseList(); // TODO

        writer.writeEndRow();
        writer.incrRowNum();

        return true;
    }

//    private boolean parseList() throws PreparePartsException {
//        try {
//
//            long time = 0;
//            for (int i = 0; i < allSize; i++) {
//                if (i == aliasTimeIndex) {
//                    time = ((Number) row.get(i)).longValue();
//                }
//
//                // i is included in extractedColumnIndexes?
//                boolean included = false;
//                for (Integer j : extractedColumnIndexes) {
//                    if (i == j) { // TODO optimize
//                        included = true;
//                        break;
//                    }
//                }
//
//                // write extracted data with writer
//                if (included) {
//                    writer.write(allColumnNames[i]);
//                    writer.write(row.get(i));
//                }
//            }
//
//            if (allSize == timeIndex) {
//                writer.write(Config.BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE);
//                if (aliasTimeIndex >= 0) {
//                    writer.write(time);
//                } else {
//                    writer.write(timeValue);
//                }
//            }
//
//            writer.writeEndRow();
//
//            writer.incrRowNum();
//            return true;
//        } catch (Exception e) {
//            throw new PreparePartsException(e);
//        }
//    }

    public void close() throws PreparePartsException {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new PreparePartsException(e);
            }
        }
    }
}