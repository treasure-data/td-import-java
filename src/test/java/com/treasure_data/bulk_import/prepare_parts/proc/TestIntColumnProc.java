package com.treasure_data.bulk_import.prepare_parts.proc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

public class TestIntColumnProc extends AbstractColumnProcTestUtil {

    private IntColumnProc proc;

    @Before
    public void initializeResources() throws Exception {
        super.initializeResources();
        proc = new IntColumnProc(index, columnName, w);
    }

    @After
    public void destroyResources() throws Exception {
        super.destroyResources();
    }

    @Override
    public AbstractColumnProc getColumnProc() {
        return proc;
    }

    @Test
    public void returnOriginalArgumentNormally() throws Exception {
        int value = 10;

        w.writeBeginRow(1);
        assertEquals(value, proc.execute(value));
        w.writeEndRow();

        hasColumn(columnName, value);
    }

    @Test
    public void gotRuntimeErrorWhenNonStringValueIsPassed() throws Exception {
        String value = "muga";

        w.writeBeginRow(1);
        try {
            assertEquals(value, proc.execute(value));
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof RuntimeException);
            assertTrue(t.getCause() instanceof PreparePartsException);
        }
        w.writeEndRow();
    }
}
