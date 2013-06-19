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

public enum ValueType {
    STRING("string") {
        @Override
        public Row.ColumnValue createColumnValue() {
            return new Row.StringColumnValue();
        }
        @Override
        public Converter getTypeConverter() {
            return Converter.STRING;
        }
    },
    INT("int") {
        @Override
        public Row.ColumnValue createColumnValue() {
            return new Row.IntColumnValue();
        }
        @Override
        public Converter getTypeConverter() {
            return Converter.INT;
        }
    },
    LONG("long") {
        @Override
        public Row.ColumnValue createColumnValue() {
            return new Row.LongColumnValue();
        }
        @Override
        public Converter getTypeConverter() {
            return Converter.LONG;
        }
    },
    DOUBLE("double") {
        @Override
        public Row.ColumnValue createColumnValue() {
            return new Row.DoubleColumnValue();
        }
        @Override
        public Converter getTypeConverter() {
            return Converter.DOUBLE;
        }
    };

    private String name;

    ValueType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public abstract Row.ColumnValue createColumnValue();
    public abstract Converter getTypeConverter();

    public static ValueType fromString(String name) {
        return StringToValueType.get(name);
    }

    private static class StringToValueType {
        private static final Map<String, ValueType> REVERSE_DICTIONARY;

        static {
            Map<String, ValueType> map = new HashMap<String, ValueType>();
            for (ValueType elem : ValueType.values()) {
                map.put(elem.getName(), elem);
            }
            REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
        }

        static ValueType get(String key) {
            return REVERSE_DICTIONARY.get(key);
        }
    }

    public static enum Converter {
        STRING("string") {
            @Override
            public void convertInto(String v, Row.ColumnValue cv) {
                ((Row.StringColumnValue) cv).setString(v);
            }
        },
        INT("int") {
            @Override
            public void convertInto(String v, Row.ColumnValue cv) {
                ((Row.IntColumnValue) cv).setInt(Integer.parseInt(v));
            }
        },
        LONG("long") {
            @Override
            public void convertInto(String v, Row.ColumnValue cv) {
                ((Row.LongColumnValue) cv).setLong(Long.parseLong(v));
            }
        },
        DOUBLE("double") {
            @Override
            public void convertInto(String v, Row.ColumnValue cv) {
                ((Row.DoubleColumnValue) cv).setDouble(Double.parseDouble(v));
            }
        };

        private String name;

        Converter(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public abstract void convertInto(String v, Row.ColumnValue cv);

        public static Converter fromString(String name) {
            return StringToTypeConverter.get(name);
        }

        private static class StringToTypeConverter {
            private static final Map<String, Converter> REVERSE_DICTIONARY;

            static {
                Map<String, Converter> map = new HashMap<String, Converter>();
                for (Converter elem : Converter.values()) {
                    map.put(elem.getName(), elem);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static Converter get(String key) {
                return REVERSE_DICTIONARY.get(key);
            }
        }
    }
}
