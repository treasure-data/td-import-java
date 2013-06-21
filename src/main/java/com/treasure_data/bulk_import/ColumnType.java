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
package com.treasure_data.bulk_import;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum ColumnType {
    STRING("string", 0) {
        @Override
        public Row.ColumnValue createColumnValue() {
            return new Row.StringColumnValue();
        }

        @Override
        public void convertTypeInto(String v, Row.ColumnValue cv) {
            cv.set(v);
        }
    },
    INT("int", 1) {
        @Override
        public Row.ColumnValue createColumnValue() {
            return new Row.IntColumnValue();
        }

        @Override
        public void convertTypeInto(String v, Row.ColumnValue cv) {
            cv.set(v);
        }
    },
    LONG("long", 2) {
        @Override
        public Row.ColumnValue createColumnValue() {
            return new Row.LongColumnValue();
        }

        @Override
        public void convertTypeInto(String v, Row.ColumnValue cv) {
            cv.set(v);
        }
    },
    DOUBLE("double", 3) {
        @Override
        public Row.ColumnValue createColumnValue() {
            return new Row.DoubleColumnValue();
        }

        @Override
        public void convertTypeInto(String v, Row.ColumnValue cv) {
            cv.set(v);
        }
    };

    private String name;
    private int index;

    ColumnType(String name, int index) {
        this.name = name;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    public abstract Row.ColumnValue createColumnValue();
    public abstract void convertTypeInto(String v, Row.ColumnValue cv);

    public static ColumnType fromInt(int index) {
        return IntToColumnType.get(index);
    }

    public static ColumnType fromString(String name) {
        return StringToColumnType.get(name);
    }

    private static class IntToColumnType {
        private static final Map<Integer, ColumnType> REVERSE_DICTIONARY;

        static {
            Map<Integer, ColumnType> map = new HashMap<Integer, ColumnType>();
            for (ColumnType elem : ColumnType.values()) {
                map.put(elem.getIndex(), elem);
            }
            REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
        }

        static ColumnType get(Integer index) {
            return REVERSE_DICTIONARY.get(index);
        }
    }

    private static class StringToColumnType {
        private static final Map<String, ColumnType> REVERSE_DICTIONARY;

        static {
            Map<String, ColumnType> map = new HashMap<String, ColumnType>();
            for (ColumnType elem : ColumnType.values()) {
                map.put(elem.getName(), elem);
            }
            REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
        }

        static ColumnType get(String key) {
            return REVERSE_DICTIONARY.get(key);
        }
    }
}
