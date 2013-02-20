package com.treasure_data.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;
import com.treasure_data.commands.bulk_import.PreparePartsResult;
import com.treasure_data.file.JSONFileParser;

public class TestJSONFileParser {

    private PreparePartsRequest request;
    private PreparePartsResult result;
    private JSONFileParser parser;
    private FileWriterTestUtil writer;

    @Before
    public void createResources() throws Exception {
        request = new PreparePartsRequest();
        result = new PreparePartsResult();

        parser = new JSONFileParser(request, result);

        writer = new FileWriterTestUtil(request, result, true);
    }

    @After
    public void deleteResources() throws Exception {
        parser.close();
        parser = null;

        writer.close();
        writer = null;
    }

    private static String toJSONMapString(String[] keys, Object[] values) {
        return JSONValue.toJSONString(createMapObject(keys, values));
    }

    private static Map<String, Object> createMapObject(String[] keys,
            Object[] values) {
        if (keys.length != values.length) {
            throw new IllegalArgumentException();
        }

        int size = keys.length;
        Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; i < size; i++) {
            map.put(keys[i], values[i]);
        }
        return map;
    }

    @Test
    public void parseIntColumn() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { 0, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { 1, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { 2, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { 0L, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { 1L, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { 2L, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseIntColumnThatNullValueIncluded() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { null, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { 1, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { null, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { null, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { 1L, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { null, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseDoubleColumn() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { 0.0, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { 1.1, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { 2.2, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { 0.0, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { 1.1, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { 2.2, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseDoubleColumnThatNullValueIncluded() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { null, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { 1.1, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { null, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { null, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { 1.1, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { null, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseStringColumn() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { "c00", 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { "c10", 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { "c20", 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { "c00", 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { "c10", 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { "c20", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseStringColumnThatNullValueIncluded() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { null, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { "c10", 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { null, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { null, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { "c10", 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { null, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseIntDoubleStringColumns() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c00", 0, 0L, 0.0, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c10", 1, 1L, 1.1, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c20", 2, 2L, 2.2, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(5);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c00", 0L, 0L, 0.0, 12345L });
        writer.setColSize(5);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c10", 1L, 1L, 1.1, 12345L });
        writer.setColSize(5);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c20", 2L, 2L, 2.2, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseColumnsThatIncludeNullValue() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c00", 0, 0L, 0.0, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { null, null, null, null, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c20", 2, 2L, 2.2, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(5);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c00", 0L, 0L, 0.0, 12345L });
        writer.setColSize(5);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { null, null, null, null, 12345L });
        writer.setColSize(5);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "time" },
                new Object[] { "c20", 2L, 2L, 2.2, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseSeveralTypeColumn() throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { "c00", 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { null, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "time" },
                new Object[] { 1, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { "c00", 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { null, 12345L });
        writer.setColSize(2);
        writer.setRow(
                new Object[] { "v0", "time" },
                new Object[] { 1L, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndAliasColumnName() throws Exception {
        // request setting
        request.setAliasTimeColumn("timestamp");

        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "timestamp" },
                new Object[] { "c00", 0, 0L, 0.0, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "timestamp" },
                new Object[] { "c10", 1, 1L, 1.1, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "timestamp" },
                new Object[] { "c20", 2, 2L, 2.2, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(6);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "timestamp", "time" },
                new Object[] { "c00", 0L, 0L, 0.0, 12345L, 12345L });
        writer.setColSize(6);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "timestamp", "time" },
                new Object[] { "c10", 1L, 1L, 1.1, 12345L, 12345L });
        writer.setColSize(6);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "timestamp", "time" },
                new Object[] { "c20", 2L, 2L, 2.2, 12345L, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndTimeValue() throws Exception {
        // request setting
        request.setAliasTimeColumn(null);
        request.setTimeValue(12345L);

        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "timestamp" },
                new Object[] { "c00", 0, 0L, 0.0, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "timestamp" },
                new Object[] { "c10", 1, 1L, 1.1, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "timestamp" },
                new Object[] { "c20", 2, 2L, 2.2, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(6);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "timestamp", "time" },
                new Object[] { "c00", 0L, 0L, 0.0, 12345L, 12345L });
        writer.setColSize(6);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "timestamp", "time" },
                new Object[] { "c10", 1L, 1L, 1.1, 12345L, 12345L });
        writer.setColSize(6);
        writer.setRow(
                new Object[] { "v0", "v1", "v2", "v3", "timestamp", "time" },
                new Object[] { "c20", 2L, 2L, 2.2, 12345L, 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void throwCmdErrorWhenParseNoTimeColumnSpecifiedColumns()
            throws Exception {
        // parser setting
        StringBuilder sb = new StringBuilder();
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "timestamp" },
                new Object[] { "c00", 0, 0L, 0.0, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "timestamp" },
                new Object[] { "c10", 1, 1L, 1.1, 12345L })).append("\n");
        sb.append(toJSONMapString(
                new String[] { "v0", "v1", "v2", "v3", "timestamp" },
                new Object[] { "c20", 2, 2L, 2.2, 12345L })).append("\n");

        String text = sb.toString();
        byte[] bytes = text.getBytes();
        try {
            parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }
}
