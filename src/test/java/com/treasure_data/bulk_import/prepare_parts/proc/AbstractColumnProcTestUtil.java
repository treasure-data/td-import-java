package com.treasure_data.bulk_import.prepare_parts.proc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfig;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;
import com.treasure_data.bulk_import.prepare_parts.writer.TestFileWriter;

@Ignore
public abstract class AbstractColumnProcTestUtil {
    protected Properties props;
    protected PrepareConfig conf;
    protected TestFileWriter w;

    protected int index;
    protected String columnName;

    protected Random rand = new Random(new Random().nextInt());

    @Before
    public void initializeResources() throws Exception {
        props = System.getProperties();

        // create prepare config
        conf = new PrepareConfig();
        conf.configure(props);

        // create file writer
        w = new TestFileWriter(conf);
        w.configure("dummy");

        index = rand.nextInt(100);
        columnName = "col" + index;
    }

    @After
    public void destroyResources() throws Exception {
        w.closeSilently();
    }

    public abstract AbstractColumnProc getColumnProc();

    @Test
    public void getFields() throws Exception {
        assertEquals(index, getColumnProc().getIndex());
        assertEquals(columnName, getColumnProc().getColumnName());
    }

    public void hasColumn(String name, Object value) {
        assertEquals(value, w.getRow().get(name));
    }

    public void executeNormalObject(Object actual, Object expected) throws Exception {
        w.writeBeginRow(1);
        assertEquals(expected, getColumnProc().execute(actual));
        w.writeEndRow();

        hasColumn(columnName, expected);
    }

    public void executeBadObject(Object value) throws Exception {
        w.writeBeginRow(1);
        try {
            assertEquals(value, getColumnProc().execute(value));
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof RuntimeException);
            assertTrue(t.getCause() instanceof PreparePartsException);
        }
        w.writeEndRow();
    }
}
