package com.treasure_data.bulk_import.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;

import com.treasure_data.bulk_import.model.DoubleColumnValue;
import com.treasure_data.bulk_import.model.IntColumnValue;
import com.treasure_data.bulk_import.model.LongColumnValue;
import com.treasure_data.bulk_import.model.StringColumnValue;
import com.treasure_data.bulk_import.model.TimeColumnValue;
import com.treasure_data.bulk_import.prepare.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare.PreparePartsException;
import com.treasure_data.bulk_import.prepare.Task;
import com.treasure_data.bulk_import.prepare.TaskResult;
import com.treasure_data.bulk_import.writer.FileWriter;

@Ignore
public class FileWriterTestUtil extends FileWriter {

    private Map<String, Object> row = new HashMap<String, Object>();
    private List<Object> columnKeyValues = new ArrayList<Object>();

    public FileWriterTestUtil(PrepareConfiguration conf) {
        super(conf);
    }

    @Override
    public void configure(Task task, TaskResult result) throws PreparePartsException {
        super.configure(task, result);
    }

    @Override
    public void writeBeginRow(int size) throws PreparePartsException {
        if (!row.isEmpty() || !columnKeyValues.isEmpty()) {
            throw new IllegalStateException("row must be empty");
        }
    }

    @Override
    public void write(String v) throws PreparePartsException {
        columnKeyValues.add(v);
    }

    @Override
    public void write(int v) throws PreparePartsException {
        columnKeyValues.add(v);
    }

    @Override
    public void write(long v) throws PreparePartsException {
        columnKeyValues.add(v);
    }

    @Override
    public void write(double v) throws PreparePartsException {
        columnKeyValues.add(v);
    }

    @Override
    public void write(TimeColumnValue filter, StringColumnValue v) throws PreparePartsException {
        String timeString = v.getString();
        long time = 0;
        try {
            time = Long.parseLong(timeString);
        } catch (Throwable t) {
            throw new PreparePartsException(String.format(
                    "'%s' could not be parsed as long type", timeString));
        }

        if (time == 0 && filter.getTimeFormat() != null) {
            time = filter.getTimeFormat().getTime(timeString);
        }

        write(time);
    }

    @Override
    public void write(TimeColumnValue filter, IntColumnValue v) throws PreparePartsException {
        v.write(this);
    }

    @Override
    public void write(TimeColumnValue filter, LongColumnValue v) throws PreparePartsException {
        v.write(this);
    }

    @Override
    public void write(TimeColumnValue filter, DoubleColumnValue v) throws PreparePartsException {
        throw new PreparePartsException("not implemented method");
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

    public void clear() throws IOException {
        if (row != null) {
            row.clear();
        }
        if (columnKeyValues != null) {
            columnKeyValues.clear();
        }
    }

    public Map<String, Object> getRow() {
        return row;
    }
}
