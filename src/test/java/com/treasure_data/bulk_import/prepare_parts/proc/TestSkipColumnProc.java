package com.treasure_data.bulk_import.prepare_parts.proc;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSkipColumnProc {

    private int index;
    private String columnName;
    private SkipColumnProc proc;
    private ColumnProc next;
    Random rand = new Random(new Random().nextInt());

    @Before
    public void initializeResources() throws Exception {
        index = rand.nextInt(100);
        columnName = "col" + index;
        next = new StringColumnProc(index, columnName, null);
        proc = new SkipColumnProc(next);
    }

    @After
    public void destroyResources() throws Exception {
        
    }

    @Test
    public void getFields() throws Exception {
        assertEquals(index, proc.getIndex());
        assertEquals(columnName, proc.getColumnName());
    }

    @Test
    public void returnOriginalArgumentNormally() throws Exception {
        Object obj = 100; // int
        assertEquals(obj, proc.execute(obj));
        obj = 0L; // long
        assertEquals(obj, proc.execute(obj));
        obj = "muga"; // String
        assertEquals(obj, proc.execute(obj));
    }
}
