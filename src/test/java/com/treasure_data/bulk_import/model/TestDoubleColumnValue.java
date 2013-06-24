package com.treasure_data.bulk_import.model;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

public class TestDoubleColumnValue extends ColumnValueTestUtil {

    @Before
    public void createResources() throws Exception {
        super.createResources();
        columnValue = new DoubleColumnValue(ColumnType.DOUBLE);
    }

    @After
    public void destroyResources() throws Exception {
        super.destroyResources();
    }

    public String invalidValue() {
        return "muga";
    }

    @Test
    public void returnNormalValues() throws Exception {
        List<Double> expecteds = new ArrayList<Double>();
        expecteds.add(Double.MAX_VALUE);
        expecteds.add(Double.MIN_VALUE);

        int numExec = rand.nextInt(10000);
        for (int i = 0; i < numExec; i++) {
            expecteds.add(rand.nextDouble());
        }

        for (int i = 0; i < expecteds.size(); i++) {
            double expected = expecteds.get(i);
            columnValue.set("" + expected);
            DoubleColumnValue actual = (DoubleColumnValue) columnValue;
            ColumnValueTestUtil.assertEquals(expected, actual);
        }
    }

    @Test
    public void throwPreparePartErrorWhenItParsesInvalidValues() throws Exception {
        try {
            columnValue.set(invalidValue());
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof PreparePartsException);
        }
    }
    
}
