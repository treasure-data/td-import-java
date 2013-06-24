package com.treasure_data.bulk_import.model;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;
import com.treasure_data.bulk_import.prepare_parts.writer.FileWriterTestUtil;

@Ignore
public class ColumnValueTestUtil<T> {

    protected Properties props;
    protected PrepareConfiguration conf;

    protected ColumnValue columnValue;

    protected List<T> expecteds = new ArrayList<T>();
    protected Random rand = new Random(new Random().nextInt());

    protected FileWriterTestUtil writer;
    protected static final String KEY = "key";

    @Before
    public void createResources() throws Exception {
        props = System.getProperties();

        conf = new PrepareConfiguration();
        conf.configure(props);

        writer = new FileWriterTestUtil(conf);
        writer.configure(null);

        createExpecteds();
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
            columnValue.parse(invalidValue());
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof PreparePartsException);
        }
    }

    @Test
    public void writeNormalValues() throws Exception {
        for (int i = 0; i < expecteds.size(); i++) {
            columnValue.parse("" + expecteds.get(i));

            writer.writeBeginRow(1);
            writer.write(KEY);
            columnValue.write(writer);
            writer.writeEndRow();
            writer.getRow().get("");
            assertWrittenValueEquals(i);
            writer.clear();
        }
        columnValue.write(writer);
    }

    public void assertWrittenValueEquals(int index) {
        throw new UnsupportedOperationException();
    }
}
