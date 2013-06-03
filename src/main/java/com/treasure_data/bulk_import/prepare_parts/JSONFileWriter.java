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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JSONFileWriter extends FileWriter {

    private Map<String, Object> record;
    private List<Object> recordElements;

    protected JSONFileWriter(PrepareConfig conf) {
        super(conf);
    }

    @Override
    public void initWriter(String infileName) throws PreparePartsException {
    }

    @Override
    public void writeBeginRow(int size) throws PreparePartsException {
        if (record != null || recordElements != null) {
            throw new IllegalStateException("record must be null");
        }
        record = new HashMap<String, Object>();
        recordElements = new ArrayList<Object>();
    }

    @Override
    public void write(Object o) throws PreparePartsException {
        recordElements.add(o);
    }

    @Override
    public void writeString(String v) throws PreparePartsException {
        recordElements.add(v);
    }

    @Override
    public void writeInt(int v) throws PreparePartsException {
        recordElements.add(v);
    }

    @Override
    public void writeLong(long v) throws PreparePartsException {
        recordElements.add(v);
    }

    @Override
    public void writeNil() throws PreparePartsException {
        recordElements.add(null);
    }

    @Override
    protected void writeEndRow() throws PreparePartsException {
        int size = recordElements.size() / 2;
        for (int i = 0; i < size; i++) {
            String key = (String) recordElements.get(2 * i);
            Object val = recordElements.get(2 * i + 1);
            record.put(key, val);
        }
    }

    public Map<String, Object> getRecord() {
        return record;
    }

    @Override
    public void close() throws IOException {
        if (record != null) {
            record.clear();
            record = null;
        }
        if (recordElements != null) {
            recordElements.clear();
            recordElements = null;
        }
    }

}
