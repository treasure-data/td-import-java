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
package com.treasure_data.bulk_import.prepare_parts.proc;

import java.util.ArrayList;
import java.util.List;

import org.supercsv.cellprocessor.ift.CellProcessor;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfig;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

public class ColumnProcessorGenerator {

    public static CellProcessor[] generateSampleCellProcessors() {
        // TODO
        return null;
    }

    public static CellProcessor[] generateCellProcessors(
            com.treasure_data.bulk_import.prepare_parts.FileWriter writer,
            String[] columnNames, PrepareConfig.ColumnType[] columnTypes,
            int timeColumnIndex, int aliasTimeColumnIndex, String timeFormat,
            long timeValue) throws PreparePartsException {
        // TODO
        int len = columnTypes.length;
        List<CellProcessor> cprocs = new ArrayList<CellProcessor>(len);
        for (int i = 0; i < len; i++) {
            cprocs.add(generateCellProcessor(i, columnNames[i], columnTypes[i], writer));
        }
        return cprocs.toArray(new CellProcessor[0]);
    }

    public static ColumnProcessor[] generateColumnProcessors(String[] columnNames,
            PrepareConfig.ColumnType[] columnTypes,
            com.treasure_data.bulk_import.prepare_parts.FileWriter writer)
                    throws PreparePartsException {
        int len = columnTypes.length;
        List<ColumnProcessor> cprocs = new ArrayList<ColumnProcessor>(len);
        for (int i = 0; i < len; i++) {
            cprocs.add(generateColumnProcessor(i, columnNames[i], columnTypes[i], writer));
        }
        return cprocs.toArray(new ColumnProcessor[0]);
    }

    public static CellProcessor generateCellProcessor(int index, String columnName,
            PrepareConfig.ColumnType columnType,
            com.treasure_data.bulk_import.prepare_parts.FileWriter writer)
                    throws PreparePartsException {
        return (CellProcessor) generateColumnProcessor(index, columnName, columnType, writer);
    }

    public static ColumnProcessor generateColumnProcessor(int index, String columnName,
            PrepareConfig.ColumnType columnType,
            com.treasure_data.bulk_import.prepare_parts.FileWriter writer)
                    throws PreparePartsException {
        if (columnType == PrepareConfig.ColumnType.INT) {
            return new CSVIntColumnProc(index, columnName, writer);
        } else if (columnType == PrepareConfig.ColumnType.LONG) {
            return new CSVLongColumnProc(index, columnName, writer);
        } else if (columnType == PrepareConfig.ColumnType.DOUBLE) {
            return new CSVDoubleColumnProc(index, columnName, writer);
        } else if (columnType == PrepareConfig.ColumnType.STRING) {
            return new CSVStringColumnProc(index, columnName, writer);
        } else if (columnType == PrepareConfig.ColumnType.TIME) {
            throw new UnsupportedOperationException();
        } else { // otherwise
            throw new UnsupportedOperationException();
        }
    }

    public static ColumnProcessor generateTimeColumnProcessor(
            com.treasure_data.bulk_import.prepare_parts.FileWriter writer,
            int aliasTimeColumnIndex, String timeFormat, long timeValue) { // TODO should change timeformat
        if (aliasTimeColumnIndex < 0) {
            return new CSVTimeValueColumnProc(timeValue, writer);
        } else {
            return new CSVAliasTimeColumnProc(aliasTimeColumnIndex, timeFormat, writer);
        }
    }
}
