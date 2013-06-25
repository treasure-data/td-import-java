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
package com.treasure_data.bulk_import.model;

public class ColumnSampling {
    private int sampleRow;
    private int[] scores = new int[] { 0, 0, 0, 0 };

    public ColumnSampling(int sampleRow) {
        this.sampleRow = sampleRow;
    }

    public void set(String value) {
        if (value == null) {
            // any score are not changed
            return;
        }

        // value looks like String object?
        scores[ColumnType.STRING.getIndex()] += 1;

        // value looks like Double object?
        try {
            Double.parseDouble((String) value);
            scores[ColumnType.DOUBLE.getIndex()] += 1;
        } catch (NumberFormatException e) {
            // ignore
        }

        // value looks like Long object?
        try {
            Long.parseLong((String) value);
            scores[ColumnType.LONG.getIndex()] += 1;
        } catch (NumberFormatException e) {
            // ignore
        }

        // value looks like Integer object?
        try {
            Integer.parseInt((String) value);
            scores[ColumnType.INT.getIndex()] += 1;
        } catch (NumberFormatException e) {
            // ignore
        }
    }

    public ColumnType getColumnType() {
        int max = -sampleRow;
        int maxIndex = 0;
        for (int i = 0; i < scores.length; i++) {
            if (max < scores[i]) {
                max = scores[i];
                maxIndex = i;
            }
        }
        return ColumnType.fromInt(maxIndex);
    }
}