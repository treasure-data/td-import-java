package com.treasure_data.bulk_import.model;

import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestColumnSampling {

    protected Random rand = new Random(new Random().nextInt());

    protected int numRows;
    protected ColumnType expected;
    protected ColumnSampling sampling;

    @Before
    public void createResources() throws Exception {
        numRows = rand.nextInt(100) + 1;
        sampling = new ColumnSampling(numRows);
    }

    @After
    public void destroyResources() throws Exception {
    }

    @Test
    public void workStringValuesNormally() throws Exception {
        expected = ColumnType.STRING;
        workValuesNormally(expected);
    }

    @Test
    public void workLongValuesNormally() throws Exception {
        expected = ColumnType.LONG;
        workValuesNormally(expected);
    }

    @Test
    public void workDoubleValuesNormally() throws Exception {
        expected = ColumnType.DOUBLE;
        workValuesNormally(expected);
    }

    private void workValuesNormally(ColumnType expected) throws Exception {
        String[] values = getValues(numRows, expected);
        for (int i = 0; i < numRows; i++) {
            sampling.parse(values[i]);
        }
        ColumnType actual = sampling.getRank();
        Assert.assertEquals(expected, actual);
    }

    private String[] getValues(int num, ColumnType type) {
        if (type.equals(ColumnType.STRING)) {
            return getStringValues(num);
        } else if (type.equals(ColumnType.LONG)) {
            return getLongValues(num);
        } else if (type.equals(ColumnType.DOUBLE)) {
            return getDoubleValues(num);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private String[] getStringValues(int num) {
        String[] values = new String[num];
        for (int i = 0; i < values.length; i++) {
            values[i] = "muga:" + rand.nextInt();
        }
        return values;
    }

    private String[] getLongValues(int num) {
        String[] values = new String[num];
        for (int i = 0; i < values.length; i++) {
            values[i] = "" + rand.nextLong();
        }
        return values;
    }

    private String[] getDoubleValues(int num) {
        String[] values = new String[num];
        for (int i = 0; i < values.length; i++) {
            values[i] = "" + rand.nextDouble();
        }
        return values;
    }
}
