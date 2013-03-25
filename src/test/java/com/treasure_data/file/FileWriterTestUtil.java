package com.treasure_data.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.Ignore;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;
import com.treasure_data.commands.bulk_import.PreparePartsResult;

@Ignore
public class FileWriterTestUtil
        extends com.treasure_data.file.FileWriter<PreparePartsRequest, PreparePartsResult> {
    private static final Logger LOG = Logger.getLogger(FileWriterTestUtil.class
            .getName());

    private List<Integer> expectedSizeList;
    private boolean isMap;

    private List<List<Object>> expectedList;
    private List<Object> gotList;
    private List<Map<Object, Object>> expectedMapList;
    private Map<Object, Object> gotMap;
    private Object rowKey;

    public FileWriterTestUtil(PreparePartsRequest request,
            PreparePartsResult result) throws CommandException {
        this(request, result, false);
    }

    public FileWriterTestUtil(PreparePartsRequest request,
            PreparePartsResult result, boolean isMap) throws CommandException {
        super(request, result);
        this.isMap = isMap;
        expectedSizeList = new ArrayList<Integer>();
        if (!isMap) { // list
            expectedList = new ArrayList<List<Object>>();
            gotList = new ArrayList<Object>();
        } else { // map
            expectedMapList = new ArrayList<Map<Object, Object>>();
            gotMap = new HashMap<Object, Object>();
        }
    }

    @Override
    public void initWriter(String infileName)
            throws CommandException {
        // do nothing
    }

    public void setColSize(int colSize) {
        expectedSizeList.add(colSize);
    }

    @Override
    public void writeBeginRow(int got) throws CommandException {
        int expected = expectedSizeList.remove(0);
        assertEquals(expected, got);
    }

    public void setRow(Object[] row) {
        if (isMap) { // map
            throw new UnsupportedOperationException();
        }

        List<Object> list = new ArrayList<Object>();
        for (Object r : row) {
            list.add(r);
        }
        expectedList.add(list);
    }

    public void setRow(Object[] rowKeys, Object[] rowValues) {
        if (!isMap) { // list
            throw new UnsupportedOperationException();
        }

        Map<Object, Object> map = new HashMap<Object, Object>();
        int s = rowKeys.length;
        for (int i = 0; i < s; i++) {
            map.put(rowKeys[i], rowValues[i]);
        }
        expectedMapList.add(map);
    }

    @Override
    public void write(Object got) throws CommandException {
        if (!isMap) { // list
            gotList.add(got);
        } else { // map
            if (rowKey != null) {
                gotMap.put(rowKey, got);
                rowKey = null;
            } else {
                rowKey = got;
            }
        }
    }

    @Override
    public void writeString(String got) throws CommandException {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void writeInt(int got) throws CommandException {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void writeLong(long got) throws CommandException {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void writeNil() throws CommandException {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void writeEndRow() throws CommandException {
        if (!isMap) { // list
            List<Object> expected = expectedList.remove(0);
            assertArrayEquals(
                    expected.toArray(new Object[0]),
                    gotList.toArray(new Object[0]));
        } else { // map
            Map<Object, Object> expected = expectedMapList.remove(0);
            for (Map.Entry<Object, Object> e : gotMap.entrySet()) {
                Object gotKey = e.getKey();
                assertEquals(expected.get(gotKey), e.getValue());
            }
        }

        if (!isMap) { // list
            gotList.clear();
        } else { // map
            gotMap.clear();
        }
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

}
