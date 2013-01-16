package com.treasure_data.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.Config;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public class TestCSVFileParser {

    static class MockFileWriter extends MsgpackGZIPFileWriter {
        private List<Integer> colSizeList;
        private List<Object> objectList;

        public MockFileWriter(PreparePartsRequest request)
                throws CommandException {
            super(request, null);
            colSizeList = new ArrayList<Integer>();
            objectList = new ArrayList<Object>();
        }

        @Override
        public void initWriter(PreparePartsRequest request, String infileName)
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
        public void close() throws CommandException {
            // do nothing
        }

        @Override
        public void closeSilently() {
            // do nothing
        }
    }

    @Test
    public void parseHeaderlessCSVText() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1,v2,time");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES,
                "string,string,string,long");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));
        PreparePartsRequest request = new PreparePartsRequest(new String[0], props);

        String text = "c00,c01,c02,12345\n" +
                      "c10,c11,c12,12345\n" +
                      "c20,c21,c22,12345\n";
        byte[] bytes = text.getBytes();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        CSVFileParser p = new CSVFileParser(request, in);

        MockFileWriter w = new MockFileWriter(request);
        w.setColSize(4);
        w.setRow(new Object[] { "v0", "c00", "v1", "c01", "v2", "c02", "time", 12345L });
        w.setColSize(4);
        w.setRow(new Object[] { "v0", "c10", "v1", "c11", "v2", "c12", "time", 12345L });
        w.setColSize(4);
        w.setRow(new Object[] { "v0", "c20", "v1", "c21", "v2", "c22", "time", 12345L });

        assertTrue(p.parseRow(w));
        assertTrue(p.parseRow(w));
        assertTrue(p.parseRow(w));
        assertFalse(p.parseRow(w));

        p.close();
        w.close();
    }

    @Test
    public void parseHeaderedCSVText() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES,
                "string,string,string,long");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));
        PreparePartsRequest request = new PreparePartsRequest(new String[0], props);

        String text = "v0,v1,v2,time\n" +
                      "c00,c01,c02,12345\n" +
                      "c10,c11,c12,12345\n" +
                      "c20,c21,c22,12345\n";
        byte[] bytes = text.getBytes();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        CSVFileParser p = new CSVFileParser(request, in);

        MockFileWriter w = new MockFileWriter(request);
        w.setColSize(4);
        w.setRow(new Object[] { "v0", "c00", "v1", "c01", "v2", "c02", "time", 12345L });
        w.setColSize(4);
        w.setRow(new Object[] { "v0", "c10", "v1", "c11", "v2", "c12", "time", 12345L });
        w.setColSize(4);
        w.setRow(new Object[] { "v0", "c20", "v1", "c21", "v2", "c22", "time", 12345L });

        assertTrue(p.parseRow(w));
        assertTrue(p.parseRow(w));
        assertTrue(p.parseRow(w));
        assertFalse(p.parseRow(w));

        p.close();
        w.close();
    }
}
