package com.treasure_data.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;
import com.treasure_data.commands.bulk_import.PreparePartsResult;

public class OneLineJSONRecordWriter extends FileWriter<PreparePartsRequest, PreparePartsResult> {

    private Map<String, Object> record;
    private List<Object> recordElements;

    protected OneLineJSONRecordWriter(PreparePartsRequest request)
            throws CommandException {
        super(request);
    }

    @Override
    public void initWriter(String infileName) throws CommandException {
    }

    @Override
    public void writeBeginRow(int size) throws CommandException {
        if (record != null || recordElements != null) {
            throw new IllegalStateException("record must be null");
        }
        record = new HashMap<String, Object>();
        recordElements = new ArrayList<Object>();
    }

    @Override
    public void write(Object o) throws CommandException {
        recordElements.add(o);
    }

    @Override
    protected void writeEndRow() throws CommandException {
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
