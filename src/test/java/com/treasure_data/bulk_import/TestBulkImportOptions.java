package com.treasure_data.bulk_import;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import joptsimple.OptionSet;

import org.junit.Before;
import org.junit.Test;

public class TestBulkImportOptions {

    protected Properties props;
    protected BulkImportOptions actualOpts;

    @Before
    public void createResources() throws Exception {
        props = System.getProperties();
        actualOpts = new BulkImportOptions();
    }

    @Test
    public void testPrepareOptions() throws Exception {
        actualOpts.initPrepareOptionParser(props);
        final String[] args = new String[] {
                "-f", "csv_format",
                "-C", "gzip_compression",
                "foo", "bar"
        };
        actualOpts.setOptions(args);

        assertOptionEquals("f", "csv_format", actualOpts);
        assertOptionEquals("format", "csv_format", actualOpts);

        assertOptionEquals("C", "gzip_compression", actualOpts);
        assertOptionEquals("compress", "gzip_compression", actualOpts);
    }

    private void assertOptionEquals(String expectedName, Integer expectedArg,
            BulkImportOptions actual) throws Exception {
        assertOptionCommonEquals(expectedName, actual);
        assertEquals(expectedArg, (Integer) actual.getOptions().valueOf(expectedName));
    }

    private void assertOptionEquals(String expectedName, Long expectedArg,
            BulkImportOptions actual) throws Exception {
        assertOptionCommonEquals(expectedName, actual);
        assertEquals(expectedArg, (Long) actual.getOptions().valueOf(expectedName));
    }

    private void assertOptionEquals(String expectedName, String expectedArg,
            BulkImportOptions actual) throws Exception {
        assertOptionCommonEquals(expectedName, actual);
        assertEquals(expectedArg, actual.getOptions().valueOf(expectedName));
    }

    private void assertOptionCommonEquals(String expectedName,
            BulkImportOptions actual) throws Exception {
        OptionSet set = actual.getOptions();
        assertTrue(set.has(expectedName));
        assertTrue(set.hasArgument(expectedName));
    }

}
