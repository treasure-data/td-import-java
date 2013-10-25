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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.treasure_data.td_import.prepare.PreparePartsException;
import com.treasure_data.td_import.writer.FileWriter;

public enum ColumnType {
    STRING("string", 0) {
        @Override
        public ColumnValue createColumnValue() {
            return new StringColumnValue(this);
        }

        @Override
        public void convertType(String v, ColumnValue into)
                throws PreparePartsException {
            into.parse(v);
        }

        @Override
        public void setColumnValue(Object v, ColumnValue cv)
                throws PreparePartsException {
            cv.set(v);
        }

        @Override
        public void filterAndWrite(ColumnValue v, TimeColumnValue filter, FileWriter with)
                throws PreparePartsException {
            with.write(filter, (StringColumnValue) v);
        }
    },
    INT("int", 4) {
        @Override
        public ColumnValue createColumnValue() {
            return new IntColumnValue(this);
        }

        @Override
        public void convertType(String v, ColumnValue into)
                throws PreparePartsException {
            into.parse(v);
        }

        @Override
        public void setColumnValue(Object v, ColumnValue cv)
                throws PreparePartsException {
            cv.set(v);
        }

        @Override
        public void filterAndWrite(ColumnValue v, TimeColumnValue filter, FileWriter with)
                throws PreparePartsException {
            with.write(filter, (IntColumnValue) v);
        }
    },
    DOUBLE("double", 2) {
        @Override
        public ColumnValue createColumnValue() {
            return new DoubleColumnValue(this);
        }

        @Override
        public void convertType(String v, ColumnValue into)
                throws PreparePartsException {
            into.parse(v);
        }

        @Override
        public void setColumnValue(Object v, ColumnValue cv)
                throws PreparePartsException {
            cv.set(v);
        }

        @Override
        public void filterAndWrite(ColumnValue v, TimeColumnValue filter, FileWriter with)
                throws PreparePartsException {
            with.write(filter, (DoubleColumnValue) v);
        }
    },
    FLOAT("float", 1) {
        @Override
        public ColumnValue createColumnValue() {
            return new FloatColumnValue(this);
        }

        @Override
        public void convertType(String v, ColumnValue into)
                throws PreparePartsException {
            into.parse(v);
        }

        @Override
        public void setColumnValue(Object v, ColumnValue cv)
                throws PreparePartsException {
            cv.set(v);
        }

        @Override
        public void filterAndWrite(ColumnValue v, TimeColumnValue filter, FileWriter with)
                throws PreparePartsException {
            with.write(filter, (FloatColumnValue) v);
        }
    },
    LONG("long", 5) {
        @Override
        public ColumnValue createColumnValue() {
            return new LongColumnValue(this);
        }

        @Override
        public void convertType(String v, ColumnValue into)
                throws PreparePartsException {
            into.parse(v);
        }

        @Override
        public void setColumnValue(Object v, ColumnValue cv)
                throws PreparePartsException {
            cv.set(v);
        }

        @Override
        public void filterAndWrite(ColumnValue v, TimeColumnValue filter, FileWriter with)
                throws PreparePartsException {
            with.write(filter, (LongColumnValue) v);
        }
    },
    BIGINT("bigint", 3) {
        @Override
        public ColumnValue createColumnValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void convertType(String v, ColumnValue into)
                throws PreparePartsException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setColumnValue(Object v, ColumnValue cv)
                throws PreparePartsException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void filterAndWrite(ColumnValue v, TimeColumnValue filter, FileWriter with)
                throws PreparePartsException {
            throw new UnsupportedOperationException();
        }
    },
    ARRAY("array", 6) {
        @Override
        public ColumnValue createColumnValue() {
            return new ArrayColumnValue(this);
        }

        @Override
        public void convertType(String v, ColumnValue into)
                throws PreparePartsException {
            into.parse(v);
        }

        @Override
        public void setColumnValue(Object v, ColumnValue cv)
                throws PreparePartsException {
            cv.set(v);
        }

        @Override
        public void filterAndWrite(ColumnValue v, TimeColumnValue filter, FileWriter with)
                throws PreparePartsException {
            with.write(filter, (ArrayColumnValue) v);
        }
    },
    MAP("map", 7) {
        @Override
        public ColumnValue createColumnValue() {
            return new MapColumnValue(this);
        }

        @Override
        public void convertType(String v, ColumnValue into)
                throws PreparePartsException {
            into.parse(v);
        }

        @Override
        public void setColumnValue(Object v, ColumnValue cv)
                throws PreparePartsException {
            cv.set(v);
        }

        @Override
        public void filterAndWrite(ColumnValue v, TimeColumnValue filter, FileWriter with)
                throws PreparePartsException {
            with.write(filter, (MapColumnValue) v);
        }
    }
    ;

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

    public abstract ColumnValue createColumnValue();

    public abstract void convertType(String v, ColumnValue into)
            throws PreparePartsException;

    public abstract void setColumnValue(Object v, ColumnValue cv)
            throws PreparePartsException;

    public abstract void filterAndWrite(ColumnValue v, TimeColumnValue filter,
            FileWriter with) throws PreparePartsException;

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
