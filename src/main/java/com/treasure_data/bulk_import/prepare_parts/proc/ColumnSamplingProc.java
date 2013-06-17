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

import static com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration.ColumnType.DOUBLE;
import static com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration.ColumnType.INT;
import static com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration.ColumnType.LONG;
import static com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration.ColumnType.STRING;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.util.CsvContext;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

public class ColumnSamplingProc extends AbstractColumnProc {

    public static PrepareConfiguration.ColumnType getColumnType(CellProcessor cellProc) {
        if (!(cellProc instanceof ColumnSamplingProc)) {
            throw new IllegalArgumentException();
        }
        return ((ColumnSamplingProc) cellProc).getColumnType();
    }

    public static PrepareConfiguration.ColumnType getColumnType(ColumnProc colProc) {
        if (!(colProc instanceof ColumnSamplingProc)) {
            throw new IllegalArgumentException();
        }
        return ((ColumnSamplingProc) colProc).getColumnType();
    }

    private int sampleRow;
    private int[] scores = new int[] { 0, 0, 0, 0 };

    public ColumnSamplingProc(int index, String columnName, int sampleRow) {
        super(index, columnName, null);
        this.sampleRow = sampleRow;
    }

    public PrepareConfiguration.ColumnType getColumnType() {
        int max = -sampleRow;
        int maxIndex = 0;
        for (int i = 0; i < scores.length; i++) {
            if (max < scores[i]) {
                max = scores[i];
                maxIndex = i;
            }
        }
        return PrepareConfiguration.ColumnType.fromInt(maxIndex);
    }

    @Override
    public Object execute(final Object value, final CsvContext context) {
        if (value == null) {
            // any score are not changed
            return null;
        }

        // value looks like String object?
        if (value instanceof String) {
            scores[STRING.index()] += 1;
        } else if (value instanceof Number) {
            scores[STRING.index()] += 1;
        }

        // value looks like Double object?
        if (value instanceof Double) {
            scores[DOUBLE.index()] += 1;
        } else if (value instanceof String) {
            try {
                Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        // value looks like Long object?
        if (value instanceof Long) {
            scores[LONG.index()] += 1;
        } else if (value instanceof String) {
            try {
                Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        // value looks like Integer object?
        if (value instanceof Integer) {
            scores[INT.index()] += 1;
        } else if (value instanceof String) {
            try {
                Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        return value;
    }

    @Override
    public Object executeValue(final Object value)
            throws PreparePartsException {
        throw new UnsupportedOperationException("fatal error");
    }
}
