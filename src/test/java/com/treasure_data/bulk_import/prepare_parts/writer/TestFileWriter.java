package com.treasure_data.bulk_import.prepare_parts.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfig;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

@Ignore
public class TestFileWriter extends FileWriter {

    private Map<String, Object> row;
    private List<Object> columnKeyValues;

    public TestFileWriter(PrepareConfig conf) {
        super(conf);
    }

    @Override
    public void configure(String infileName) throws PreparePartsException {
    }

    @Override
    public void writeBeginRow(int size) throws PreparePartsException {
        if (row != null || columnKeyValues != null) {
            throw new IllegalStateException("row must be null");
        }
        row = new HashMap<String, Object>();
        columnKeyValues = new ArrayList<Object>();
    }

    @Override
    public void write(Object v) throws PreparePartsException {
        columnKeyValues.add(v);
    }

    @Override
    public void writeString(String v) throws PreparePartsException {
        columnKeyValues.add(v);
    }

    @Override
    public void writeInt(int v) throws PreparePartsException {
        columnKeyValues.add(v);
    }

    @Override
    public void writeLong(long v) throws PreparePartsException {
        columnKeyValues.add(v);
    }

    @Override
    public void writeDouble(double v) throws PreparePartsException {
        columnKeyValues.add(v);
    }

    @Override
    public void writeNil() throws PreparePartsException {
        columnKeyValues.add(null);
    }

    @Override
    public void writeEndRow() throws PreparePartsException {
        int size = columnKeyValues.size() / 2;
        for (int i = 0; i < size; i++) {
            String key = (String) columnKeyValues.get(2 * i);
            Object val = columnKeyValues.get(2 * i + 1);
            row.put(key, val);
        }
    }

    @Override
    public void close() throws IOException {
        if (row != null) {
            row.clear();
            row = null;
        }
        if (columnKeyValues != null) {
            columnKeyValues.clear();
            columnKeyValues = null;
        }
    }

    public Map<String, Object> getRow() {
        return row;
    }
}
