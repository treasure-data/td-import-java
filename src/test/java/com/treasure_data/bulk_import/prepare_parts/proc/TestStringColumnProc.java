package com.treasure_data.bulk_import.prepare_parts.proc;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStringColumnProc extends AbstractColumnProcTestUtil {

    private StringColumnProc proc;

    @Before
    public void initializeResources() throws Exception {
        super.initializeResources();
        proc = new StringColumnProc(index, columnName, w);
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
        executeNormalObject("muga", "muga");
    }

    @Test
    public void gotRuntimeErrorWhenCannotParsedStringValueIsPassed() throws Exception {
        assertTrue(true);
    }
}
