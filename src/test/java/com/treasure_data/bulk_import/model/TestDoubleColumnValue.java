package com.treasure_data.bulk_import.model;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

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

    @Override
    public void assertWrittenValueEquals(int index) {
        Assert.assertEquals(expecteds.get(index), (Double) writer.getRow().get(KEY));
    }

    @Override
    public void prepareMockForWriting() throws Exception {
        writer = spy(writer);
        doThrow(new PreparePartsException("")).when(writer).write(any(double.class));
    }
}