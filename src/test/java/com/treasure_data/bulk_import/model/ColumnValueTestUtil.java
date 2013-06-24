package com.treasure_data.bulk_import.model;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

@Ignore
public class ColumnValueTestUtil<T> {

    protected ColumnValue columnValue;

    protected List<T> expecteds = new ArrayList<T>();
    protected Random rand = new Random(new Random().nextInt());

    @Before
    public void createResources() throws Exception {
    }

    @After
    public void destroyResources() throws Exception {
    }

    public void createExpecteds() {
        throw new UnsupportedOperationException();
    }

    public String invalidValue() {
        return "muga";
    }

    @Test
    public void returnNormalValues() throws Exception {
        throw new UnsupportedOperationException();
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
