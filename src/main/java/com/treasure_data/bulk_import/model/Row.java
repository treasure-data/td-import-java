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


public class Row {
    private ColumnValue[] values;
    private boolean needAdditionalTimeColumn = false;
    private TimeColumnValue timeColumnValue;
    private int timeColumnIndex = -1;

    public Row(ColumnValue[] values, TimeColumnValue timeColumnValue) {
        this.values = values;
        needAdditionalTimeColumn =
                timeColumnValue instanceof AliasTimeColumnValue ||
                timeColumnValue instanceof TimeValueTimeColumnValue;
        if (!needAdditionalTimeColumn) {
            timeColumnIndex = ((TimeColumnValue) timeColumnValue).getIndex();
        }
        this.timeColumnValue = timeColumnValue;
    }

    public void setValues(ColumnValue[] values) {
        this.values = values;
    }

    public void setValue(int i, ColumnValue value) {
        this.values[i] = value;
    }

    public ColumnValue[] getValues() {
        return values;
    }

    public ColumnValue getValue(int i) {
        return values[i];
    }

    public boolean needAdditionalTimeColumn() {
        return needAdditionalTimeColumn;
    }

    public int getTimeColumnIndex() {
        return timeColumnIndex;
    }

    public TimeColumnValue getTimeColumnValue() {
        return timeColumnValue;
    }
}
