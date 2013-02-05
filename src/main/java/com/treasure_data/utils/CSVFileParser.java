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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.catalina.util.Strftime;
import org.json.simple.JSONValue;
import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ConvertNullTo;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.Config;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public class CSVFileParser extends FileParser {
    static final int INT = 0;
    static final int LONG = 1;
    static final int DOUBLE = 2;
    static final int STRING = 3;

    private static final Logger LOG = Logger.getLogger(CSVFileParser.class
            .getName());

    static class CellProcessorGen {
        public CellProcessor[] genForSampleReader(String[] typeHints,
                int sampleRowSize, int sampleHintScore) throws CommandException {
            TypeSuggestionProcessor[] cprocs = new TypeSuggestionProcessor[typeHints.length];
            for (int i = 0; i < cprocs.length; i++) {
                cprocs[i] = new TypeSuggestionProcessor(sampleRowSize,
                        sampleHintScore);
                cprocs[i].addHint(typeHints[i]);
            }
            return cprocs;
        }

        public CellProcessor[] gen(int[] columnTypes) throws CommandException {
            int len = columnTypes.length;
            List<CellProcessor> cprocs = new ArrayList<CellProcessor>(len);
            for (int i = 0; i < len; i++) {
                CellProcessor cproc;
                int t = columnTypes[i];
                switch (t) { // override 'optional' ?
                case INT:
                    cproc = new ConvertNullTo(null, new ParseInt());
                    break;
                case LONG:
                    cproc = new ConvertNullTo(null, new ParseLong());
                    break;
                case DOUBLE:
                    cproc = new ConvertNullTo(null, new ParseDouble());
                    break;
                case STRING:
                    cproc = new Optional();
                    break;
                default:
                    throw new CommandException("unsuported type: " + t);
                }
                cprocs.add(cproc);
            }
            return cprocs.toArray(new CellProcessor[0]);
        }
    }

    static class TypeSuggestionProcessor extends CellProcessorAdaptor {
        private int[] scores = new int[] { 0, 0, 0, 0 };
        private int rowSize;
        private int hintScore;

        TypeSuggestionProcessor(int rowSize, int hintScore) {
            this.rowSize = rowSize;
            this.hintScore = hintScore;
        }

        void addHint(String typeHint) throws CommandException {
            if (typeHint.equals("string")) {
                scores[STRING] += hintScore;
            } else if (typeHint.equals("int")) {
                scores[INT] += hintScore;
            } else if (typeHint.equals("long")) {
                scores[LONG] += hintScore;
            } else if (typeHint.equals("double")) {
                scores[DOUBLE] += hintScore;
            } else {
                throw new CommandException("Unsupported type: " + typeHint);
            }
        }

        int getSuggestedType() {
            int max = -rowSize;
            int maxIndex = 0;
            for (int i = 0; i < scores.length; i++) {
                if (max < scores[i]) {
                    max = scores[i];
                    maxIndex = i;
                }
            }
            return maxIndex;
        }

        void printScores() {
            for (int i = 0; i < scores.length; i++) {
                System.out.println(scores[i]);
            }
        }

        int getScore(int type) {
            if (type < 0 || type >= 4) {
                throw new ArrayIndexOutOfBoundsException(type);
            }
            return scores[type];
        }

        @Override
        public Object execute(Object value, CsvContext context) {
            if (value == null) {
                // any score are not changed
                return null;
            }

            Object result = null;

            // value looks like String object?
            scores[STRING] += 1;

            // value looks like Double object?
            if (value instanceof Double) {
                result = (Double) value;
                scores[DOUBLE] += 1;
            } else {
                try {
                    result = Double.parseDouble((String) value);
                    scores[DOUBLE] += 1;
                } catch (NumberFormatException e) {
                    // ignore
                }
            }


            if (value instanceof Long) {
                result = (Long) value;
                scores[LONG] += 1;
            } else if (value instanceof String) {
                try {
                    result = Long.parseLong((String) value);
                    scores[LONG] += 1;
                } catch (NumberFormatException e) {
                    // ignore
                }
            }

            // value looks like Integer object?
            if (value instanceof Integer) {
                result = (Integer) value;
                scores[INT] += 1;
            } else if (value instanceof String) {
                try {
                    result = Integer.parseInt((String) value);
                    scores[INT] += 1;
                } catch (NumberFormatException e) {
                    // ignore
                }
            }

            return next.execute(result, context);
        }
    }

    private static class ExtStrftime extends Strftime {
        public ExtStrftime(String origFormat) {
            super(origFormat);
        }

        public SimpleDateFormat getSimpleDateFormat() {
            return simpleDateFormat;
        }
    }

    private static class ParseStrftimeDate extends CellProcessorAdaptor
            implements StringCellProcessor {
        private static SimpleDateFormat simpleFormat;

        public ParseStrftimeDate(String dateFormat) {
            super();
            if (dateFormat == null) {
                throw new NullPointerException("dateFormat should not be null");
            }
            simpleFormat = new ExtStrftime(dateFormat).getSimpleDateFormat();
        }

        /**
         * {@inheritDoc}
         *
         * @throws SuperCsvCellProcessorException
         *             if value is null, isn't a String, or can't be parsed to a Date
         */
        public Object execute(final Object value, final CsvContext context) {
            validateInputNotNull(value, context);

            if (!(value instanceof String)) {
                throw new SuperCsvCellProcessorException(String.class, value,
                        context, this);
            }

            try {
                Long result = simpleFormat.parse((String) value).getTime() / 1000;
                return next.execute(result, context);
            } catch (final ParseException e) {
                throw new SuperCsvCellProcessorException(String.format(
                        "'%s' could not be parsed as a Date", value), context,
                        this, e);
            }
        }
    }

    private PreparePartsRequest request;
    private ICsvListReader reader;
    private int timeIndex = -1;
    private Long timeValue = new Long(-1);
    private int aliasTimeIndex = -1;
    private String[] columnNames;

    private String[] columnTypeHints;
    private int[] columnTypes;

    private CellProcessor[] cprocessors;

    public CSVFileParser(PreparePartsRequest request) throws CommandException {
        this.request = request;
    }

    @Override
    public void doPreExecute(InputStream in) throws CommandException {
        // TODO more testing

        // encoding
        final CharsetDecoder decoder;
        String encodingName = request.getEncoding();
        if (encodingName.equals("utf-8")) {
            decoder = Charset.forName("UTF-8").newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
        } else {
            // TODO any more...
            throw new CommandException(new UnsupportedOperationException());
        }

        // CSV preference
        CsvPreference pref = new CsvPreference.Builder('"',
                request.getDelimiterChar(), request.getNewline()).build();

        // create sample reader
        CsvListReader sampleReader = new CsvListReader(new InputStreamReader(
                in, decoder), pref);
        try {
            // column name e.g. "time,name,price"
            if (request.hasColumnHeader()) {
                List<String> columnList = sampleReader.read();
                columnNames = columnList.toArray(new String[0]);
            } else {
                columnNames = request.getColumnNames();
            }

            String aliasTimeColumnName = request.getAliasTimeColumn();
            if (aliasTimeColumnName != null) {
                for (int i = 0; i < columnNames.length; i++) {
                    if (columnNames[i].equals(aliasTimeColumnName)) {
                        aliasTimeIndex = i;
                        break;
                    }
                }
            }
            for (int i = 0; i < columnNames.length; i++) {
                if (columnNames[i]
                        .equals(Config.BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE)) {
                    timeIndex = i;
                    break;
                }
            }
            if (timeIndex < 0) {
                timeValue = request.getTimeValue();
                if (aliasTimeIndex >= 0 || timeValue > 0) {
                    timeIndex = columnNames.length;
                } else {
                    throw new CommandException(
                            "Time column not found. --time-column or --time-value option is required");
                }
            }

            // "long,string,long"
            columnTypeHints = request.getColumnTypeHints();

            cprocessors = new CellProcessorGen().genForSampleReader(
                    columnTypeHints, request.getSampleRowSize(),
                    request.getSampleHintScore());

            List<Object> firstRow = null;
            boolean isFirstRow = false;
            for (int i = 0; i < request.getSampleRowSize(); i++) {
                List<Object> row = sampleReader.read(cprocessors);
                if (!isFirstRow) {
                    firstRow = row;
                    isFirstRow = true;
                }

                if (row == null || row.isEmpty()) {
                    break;
                }
            }

            columnTypes = new int[cprocessors.length];
            for (int i = 0; i < cprocessors.length; i++) {
                columnTypes[i] = ((TypeSuggestionProcessor) cprocessors[i])
                        .getSuggestedType();
            }

            // print sample row
            if (firstRow != null) {
                /**
                 * TODO #MN
                 * we should parse first row with suggested type converters
                 */
                String s = JSONValue.toJSONString(firstRow);
                LOG.info("sample row: " + s);
            }
        } catch (IOException e) {
            throw new CommandException(e);
        } finally {
            if (sampleReader != null) {
                try {
                    sampleReader.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    @Override
    public void initReader(InputStream in) throws CommandException {
        // encoding
        final CharsetDecoder decoder; // redundant code
        String encodingName = request.getEncoding();
        if (encodingName.equals("utf-8")) {
            decoder = Charset.forName("UTF-8").newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
        } else {
            // TODO any more... 'sjis', 'euc',...
            throw new CommandException(new UnsupportedOperationException());
        }

        // CSV preference
        CsvPreference pref = new CsvPreference.Builder('"',
                request.getDelimiterChar(), request.getNewline()).build();

        // create reader
        reader = new CsvListReader(new InputStreamReader(in, decoder), pref);

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

        String aliasTimeColumnName = request.getAliasTimeColumn();
        if (aliasTimeColumnName != null) {
            for (int i = 0; i < columnNames.length; i++) {
                if (columnNames[i].equals(aliasTimeColumnName)) {
                    aliasTimeIndex = i;
                    break;
                }
            }
        }
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i]
                    .equals(Config.BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE)) {
                timeIndex = i;
                break;
            }
        }
        if (timeIndex < 0) {
            timeValue = request.getTimeValue();
            if (aliasTimeIndex >= 0 || timeValue > 0) {
                timeIndex = columnNames.length;
            } else {
                throw new CommandException(
                        "Time column not found. --time-column or --time-value option is required");
            }
        }

        cprocessors = new CellProcessorGen().gen(columnTypes);
    }

    public boolean parseRow(MsgpackGZIPFileWriter w) throws CommandException {
        List<Object> row = null;
        try {
            row = reader.read(cprocessors);
        } catch (Exception e) {
            // catch IOException and SuperCsvCellProcessorException
            e.printStackTrace();

            if (errWriter != null) {
                // TODO and parsent-encoded row?
                String msg = String.format("reason: %s, line: %d",
                        e.getMessage(), getRowNum());
                errWriter.println(msg);
            }

            LOG.warning("Skip row number: " + getRowNum());
            return true;
        }

        if (row == null || row.isEmpty()) {
            return false;
        }

        // increment row number
        incrRowNum();

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("lineNo=%s, rowNo=%s, customerList=%s",
                    reader.getLineNumber(), reader.getRowNumber(),
                    row));
        }

        /** DEBUG
        System.out.println(String.format("lineNo=%s, rowNo=%s, customerList=%s",
                reader.getLineNumber(), reader.getRowNumber(), row));
         */

        try {
            int size = row.size();

            if (size == timeIndex) {
                w.writeBeginRow(size + 1);
            } else {
                w.writeBeginRow(size);
            }

            long time = 0;
            for (int i = 0; i < size; i++) {
                if (i == aliasTimeIndex) {
                    time = (Long) row.get(i);
                }

                w.write(columnNames[i]);
                w.write(row.get(i));
            }

            if (size == timeIndex) {
                w.write(Config.BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE);
                if (aliasTimeIndex >= 0) {
                    w.write(time);
                } else {
                    w.write(timeValue);
                }
            }

            w.writeEndRow();

            w.incrRowNum();
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