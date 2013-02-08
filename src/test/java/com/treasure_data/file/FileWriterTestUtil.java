package com.treasure_data.file;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    private List<Integer> colSizeList;
    private List<Object> objectList;

    public FileWriterTestUtil(PreparePartsRequest request)
            throws CommandException {
        super(request);
        colSizeList = new ArrayList<Integer>();
        objectList = new ArrayList<Object>();
    }

    @Override
    public void initWriter(String infileName)
            throws CommandException {
        // do nothing
    }

    public void setColSize(int colSize) {
        colSizeList.add(colSize);
    }

    @Override
    public void writeBeginRow(int got) throws CommandException {
        int expected = colSizeList.remove(0);
        assertEquals(expected, got);
    }

    public void setRow(Object[] row) {
        for (Object c : row) {
            objectList.add(c);
        }
    }

    @Override
    public void write(Object got) throws CommandException {
        Object expected = objectList.remove(0);
        assertEquals(expected, got);
    }

    @Override
    public void writeEndRow() throws CommandException {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

}
