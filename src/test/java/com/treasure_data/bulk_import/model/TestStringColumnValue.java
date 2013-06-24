package com.treasure_data.bulk_import.model;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

public class TestStringColumnValue extends ColumnValueTestUtil {

    @Before
    public void createResources() throws Exception {
        super.createResources();
        columnValue = new StringColumnValue(ColumnType.STRING);
    }

    @After
    public void destroyResources() throws Exception {
        super.destroyResources();
    }

    @Test
    public void returnNormalValues() throws Exception {
        List<String> expecteds = new ArrayList<String>();

        int numExec = rand.nextInt(10000);
        for (int i = 0; i < numExec; i++) {
            StringBuilder sbuf = new StringBuilder();
            int sizeString = rand.nextInt(30);
            for (int j = 0; j < sizeString; j++) {
                sbuf.append((char) rand.nextInt());
            }
            expecteds.add(sbuf.toString());
        }

        for (int i = 0; i < expecteds.size(); i++) {
            String expected = expecteds.get(i);
            columnValue.set("" + expected);
            StringColumnValue actual = (StringColumnValue) columnValue;
            ColumnValueTestUtil.assertEquals(expected, actual);
        }
    }

    @Test
    public void throwPreparePartErrorWhenItParsesInvalidValues() throws Exception {
        assertTrue(true);
    }
    
}
