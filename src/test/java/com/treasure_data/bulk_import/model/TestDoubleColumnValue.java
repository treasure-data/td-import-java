package com.treasure_data.bulk_import.model;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDoubleColumnValue extends ColumnValueTestUtil<Double> {

    @Before
    public void createResources() throws Exception {
        super.createResources();
        columnValue = new DoubleColumnValue(ColumnType.DOUBLE);
    }

    @After
    public void destroyResources() throws Exception {
        super.destroyResources();
    }

    @Override
    public void createExpecteds() {
        expecteds.add(Double.MAX_VALUE);
        expecteds.add(Double.MIN_VALUE);

        int numExec = rand.nextInt(10000);
        for (int i = 0; i < numExec; i++) {
            expecteds.add(rand.nextDouble());
        }
    }

    @Test
    public void returnNormalValues() throws Exception {
        for (int i = 0; i < expecteds.size(); i++) {
            columnValue.parse("" + expecteds.get(i));
            assertColumnValueEquals(expecteds.get(i),
                    (DoubleColumnValue) columnValue);
        }
    }

    void assertColumnValueEquals(double expected, DoubleColumnValue actual) {
        Assert.assertEquals(expected, actual.getDouble(), 0);
    }
}
