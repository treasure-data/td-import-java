package com.treasure_data.file.proc;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;

import org.junit.Test;

import com.treasure_data.commands.bulk_import.CSVPreparePartsRequest;

public class TestTypeSuggestProc {

    @Test
    public void suggestStringTypeWhenReadStrings() throws Exception {
        {
            Object[] values = new Object[] { "v0", "v1", "v2", "v3", "v4" };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.INT), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.LONG), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);
            assertEquals(CSVPreparePartsRequest.ColumnType.STRING, proc.getSuggestedType());
        }
        {
            Object[] values = new Object[] { "v0", "v1", "v2", "v3", "v4" };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            proc.setType(CSVPreparePartsRequest.ColumnType.STRING);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(CSVPreparePartsRequest.ColumnType.STRING, proc.getSuggestedType());
        }
    }

    @Test
    public void suggestStringTypeWhenReadIntegers() throws Exception {
        {
            Object[] values = new Object[] { 0, 1, 2, 3, 4 };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.INT), values.length);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.LONG), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);
            assertEquals(CSVPreparePartsRequest.ColumnType.INT, proc.getSuggestedType());
        }
        {
            Object[] values = new Object[] { 0, 1, 2, 3, 4 };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            proc.setType(CSVPreparePartsRequest.ColumnType.STRING);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(CSVPreparePartsRequest.ColumnType.STRING, proc.getSuggestedType());
        }
    }

    @Test
    public void suggestStringTypeWhenReadDoubles() throws Exception {
        {
            Object[] values = new Object[] { 0.0, 1.0, 2.0, 3.0, 4.0 };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.INT), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.LONG), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), values.length);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);
            assertEquals(CSVPreparePartsRequest.ColumnType.DOUBLE, proc.getSuggestedType());
        }
        {
            Object[] values = new Object[] { 0.0, 1.0, 2.0, 3.0, 4.0 };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            proc.setType(CSVPreparePartsRequest.ColumnType.STRING);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(CSVPreparePartsRequest.ColumnType.STRING, proc.getSuggestedType());
        }
    }

    @Test
    public void suggestIntegerTypeWhenReadIntegers() throws Exception {
        {
            Object[] values = new Object[] { 0, 1, 2, 3, 4 };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.INT), values.length);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.LONG), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);
            assertEquals(CSVPreparePartsRequest.ColumnType.INT, proc.getSuggestedType());
        }
        {
            Object[] values = new Object[] { 0, 1, 2, 3, 4 };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            proc.setType(CSVPreparePartsRequest.ColumnType.INT);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(CSVPreparePartsRequest.ColumnType.INT, proc.getSuggestedType());
        }
    }

    @Test
    public void suggestIntegerTypeWhenReadStrings() throws Exception {
        {
            Object[] values = new Object[] { "0", "1", "2", "3", "4" };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.INT), values.length);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.LONG), values.length);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), values.length);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);
            assertEquals(CSVPreparePartsRequest.ColumnType.INT, proc.getSuggestedType());
        }
        {
            Object[] values = new Object[] { "0", "1", "2", "3", "4" };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            proc.setType(CSVPreparePartsRequest.ColumnType.INT);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(CSVPreparePartsRequest.ColumnType.INT, proc.getSuggestedType());
        }
    }

    @Test
    public void suggestLongTypeWhenReadIntegers() throws Exception {
        {
            Object[] values = new Object[] { 0, 1, 2, 3, 4 };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            proc.setType(CSVPreparePartsRequest.ColumnType.LONG);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(CSVPreparePartsRequest.ColumnType.LONG, proc.getSuggestedType());
        }
    }

    @Test
    public void suggestLongTypeWhenReadStrings() throws Exception {
        {
            Object[] values = new Object[] { "0", "1", "2", "3", "4" };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            proc.setType(CSVPreparePartsRequest.ColumnType.LONG);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(CSVPreparePartsRequest.ColumnType.LONG, proc.getSuggestedType());
        }
    }

    @Test
    public void suggestDoubleTypeWhenReadDoubles() throws Exception {
        {
            Object[] values = new Object[] { 0.0, 1.0, 2.0, 3.0, 4.0 };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.INT), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.LONG), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), values.length);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);
            assertEquals(CSVPreparePartsRequest.ColumnType.DOUBLE, proc.getSuggestedType());
        }
        {
            Object[] values = new Object[] { 0.0, 1.0, 2.0, 3.0, 4.0 };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            proc.setType(CSVPreparePartsRequest.ColumnType.DOUBLE);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(CSVPreparePartsRequest.ColumnType.DOUBLE, proc.getSuggestedType());
        }
    }

    @Test
    public void suggestDoubleTypeWhenReadStrings() throws Exception {
        {
            Object[] values = new Object[] { "0.0", "1.0", "2.0", "3.0", "4.0" };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.INT), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.LONG), 0);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), values.length);
            assertEquals(proc.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);
            assertEquals(CSVPreparePartsRequest.ColumnType.DOUBLE, proc.getSuggestedType());
        }
        {
            Object[] values = new Object[] { "0.0", "1.0", "2.0", "3.0", "4.0" };
            TypeSuggestProc proc = new TypeSuggestProc(values.length);
            proc.setType(CSVPreparePartsRequest.ColumnType.DOUBLE);
            for (Object value : values) {
                proc.execute(value, null);
            }
            assertEquals(CSVPreparePartsRequest.ColumnType.DOUBLE, proc.getSuggestedType());
        }
    }
}
