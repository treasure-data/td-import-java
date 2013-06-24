package com.treasure_data.bulk_import.model;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLongColumnValue extends ColumnValueTestUtil<Long> {

    @Before
    public void createResources() throws Exception {
        super.createResources();
        columnValue = new LongColumnValue(ColumnType.LONG);
    }

    @After
    public void destroyResources() throws Exception {
        super.destroyResources();
    }

    @Override
    public void createExpecteds() {
        expecteds.add(Long.MAX_VALUE);
        expecteds.add(Long.MIN_VALUE);

        int numExec = rand.nextInt(10000);
        for (int i = 0; i < numExec; i++) {
            expecteds.add(rand.nextLong());
        }
    }

    @Test
    public void returnNormalValues() throws Exception {
        for (int i = 0; i < expecteds.size(); i++) {
            columnValue.set("" + expecteds.get(i));
            assertColumnValueEquals(expecteds.get(i),
                    (LongColumnValue) columnValue);
        }
    }

    void assertColumnValueEquals(long expected, LongColumnValue actual) {
        Assert.assertEquals(expected, actual.getLong());
    }
}
