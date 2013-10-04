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
package com.treasure_data.td_import.model;

import com.treasure_data.td_import.prepare.PreparePartsException;
import com.treasure_data.td_import.writer.FileWriter;

public class LongColumnValue extends AbstractColumnValue {
    private long v;

    public LongColumnValue(ColumnType columnType) {
        super(columnType);
    }

    public void set(Object v) throws PreparePartsException {
        this.v = (Long) v;
    }

    public void parse(String v) throws PreparePartsException {
        if (v == null) {
            this.v = 0;
            return;
        }

        try {
            this.v = Long.parseLong(v);
        } catch (Exception e) {
            throw new PreparePartsException(e);
        }
    }

    public long getLong() {
        return v;
    }

    @Override
    public void write(FileWriter with) throws PreparePartsException {
        with.write(v);
    }
}