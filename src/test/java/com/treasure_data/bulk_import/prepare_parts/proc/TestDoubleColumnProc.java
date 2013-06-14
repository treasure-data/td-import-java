package com.treasure_data.bulk_import.prepare_parts.proc;


import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDoubleColumnProc extends AbstractColumnProcTestUtil {

    private DoubleColumnProc proc;

    @Before
    public void initializeResources() throws Exception {
        super.initializeResources();
        proc = new DoubleColumnProc(index, columnName, w);
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
    public void returnOriginalArgumentNormally01() throws Exception {
        executeNormalObject(10.1, 10.1);
    }

    @Test
    public void returnOriginalArgumentNormally02() throws Exception {
        executeNormalObject("10.1", 10.1);
    }

    @Test
    public void gotRuntimeErrorWhenNonStringValueIsPassed01() throws Exception {
        executeBadObject("muga");
    }

    @Test
    public void gotRuntimeErrorWhenNonStringValueIsPassed02() throws Exception {
        executeBadObject(new Date());
    }
}
