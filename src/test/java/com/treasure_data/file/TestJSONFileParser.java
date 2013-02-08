package com.treasure_data.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.commands.bulk_import.PreparePartsRequest;
import com.treasure_data.file.JSONFileParser;

public class TestJSONFileParser {

    private PreparePartsRequest request;
    private JSONFileParser parser;
    private FileWriterTestUtil writer;

    @Before
    public void createResources() throws Exception {
        request = new PreparePartsRequest();

        parser = new JSONFileParser(request);

        writer = new FileWriterTestUtil(request, true);
    }

    @After
    public void deleteResources() throws Exception {
        parser.close();
        parser = null;

        writer.close();
        writer = null;
    }

    @Test
    public void parseIntDoubleStringColumns() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        {
            Map<String, Object> row = new HashMap<String, Object>();
            row.put("v0", "c00");
            row.put("v1", 0);
            row.put("v2", 0);
            row.put("v3", 0.0);
            row.put("time", 12345);
            sb.append(JSONValue.toJSONString(row)).append("\n");
        }
        {
            Map<String, Object> row = new HashMap<String, Object>();
            row.put("v0", "c10");
            row.put("v1", 1);
            row.put("v2", 1);
            row.put("v3", 1.1);
            row.put("time", 12345);
            sb.append(JSONValue.toJSONString(row)).append("\n");
        }
        {
            Map<String, Object> row = new HashMap<String, Object>();
            row.put("v0", "c20");
            row.put("v1", 2);
            row.put("v2", 2);
            row.put("v3", 2.2);
            row.put("time", 12345);
            sb.append(JSONValue.toJSONString(row)).append("\n");
        }
        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(5);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c00", 0L, 0L, 0.0, 12345L }); // TODO
        writer.setColSize(5);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c10", 1L, 1L, 1.1, 12345L }); // TODO
        writer.setColSize(5);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c20", 2L, 2L, 2.2, 12345L }); // TODO

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

}
