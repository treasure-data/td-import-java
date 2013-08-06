package com.treasure_data.bulk_import;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;

import joptsimple.OptionSet;

import org.junit.Before;
import org.junit.Test;

public class TestBulkImportOptions {

    private final String sampleFormat = "csv";
    private final String sampleCompress = "gzip";
    private final String sampleEncoding = "udf-8";
    private final String sampleTimeColumn = "timestamp";
    private final String sampleTimeFormat = "timeformat";
    private final String sampleTimeValue = "100";
    private final String sampleOutput = "output_dir";
    private final String sampleSplitSize = "100";
    private final String sampleErrorRecordsHandling = "skip";
    private final String sampleDelimiter = ",";
    private final String sampleQuote = "DOUBLE";
    private final String sampleNewline = "CRLF";
    private final String sampleColumns = "c0,c1,c2";
    private final String sampleColumnTypes = "string,int,int";
    private final String sampleExcludeColumns = "c0,c1,c2";
    private final String sampleOnlyColumns = "c0,c1,c2";
    private final String samplePrepareParallel = "10";

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
        actualOpts.setOptions(createPrepareArguments());

        assertOptionEquals("f", sampleFormat, actualOpts);
        assertOptionEquals("format", sampleFormat, actualOpts);
        assertOptionEquals("C", sampleCompress, actualOpts);
        assertOptionEquals("compress", sampleCompress, actualOpts);
        assertOptionEquals("e", sampleEncoding, actualOpts);
        assertOptionEquals("encoding", sampleEncoding, actualOpts);
        assertOptionEquals("t", sampleTimeColumn, actualOpts);
        assertOptionEquals("time-column", sampleTimeColumn, actualOpts);
        assertOptionEquals("T", sampleTimeFormat, actualOpts);
        assertOptionEquals("time-format", sampleTimeFormat, actualOpts);
        assertOptionEquals("time-value", sampleTimeValue, actualOpts);
        assertOptionEquals("output", sampleOutput, actualOpts);
        assertOptionEquals("split-size", sampleSplitSize, actualOpts);
        assertOptionEquals("error-records-handling", sampleErrorRecordsHandling, actualOpts);
        assertOptionEquals("delimiter", sampleDelimiter, actualOpts);
        assertOptionEquals("quote", sampleQuote, actualOpts);
        assertOptionEquals("newline", sampleNewline, actualOpts);
        assertOptionEquals("column-header", actualOpts);
        assertOptionEquals("columns", sampleColumns.split(","), actualOpts);
        assertOptionEquals("column-types", sampleColumnTypes.split(","), actualOpts);
        assertOptionEquals("exclude-columns", sampleExcludeColumns.split(","), actualOpts);
        assertOptionEquals("only-columns", sampleOnlyColumns.split(","), actualOpts);
        assertOptionEquals("prepare-parallel", samplePrepareParallel, actualOpts);
    }

    private String[] createPrepareArguments() {
        return new String[] {
                "--format", sampleFormat,
                "--compress", sampleCompress,
                "--encoding", sampleEncoding,
                "--time-column", sampleTimeColumn,
                "--time-format", sampleTimeFormat,
                "--time-value", sampleTimeValue,
                "--output", sampleOutput,
                "--split-size", sampleSplitSize,
                "--error-records-handling", sampleErrorRecordsHandling,
                "--delimiter", sampleDelimiter,
                "--quote", sampleQuote,
                "--newline", sampleNewline,
                "--column-header",
                "--columns", sampleColumns,
                "--column-types", sampleColumnTypes,
                "--exclude-columns", sampleExcludeColumns,
                "--only-columns", sampleOnlyColumns,
                "--prepare-parallel", samplePrepareParallel,
        };
    }

//    @Test
//    public void testUploadOptions() throws Exception {
//        actualOpts.initUploadOptionParser(props);
//        final String[] args = new String[] {
//                "--format", sampleFormat,
//                "--compress", sampleCompress,
//                "--encoding", sampleEncoding,
//                "--time-column", sampleTimeColumn,
//                "--time-format", sampleTimeFormat,
//                "--time-value", sampleTimeValue,
//                "--output", sampleOutput,
//                "--split-size", sampleSplitSize,
//                "--error-records-handling", sampleErrorRecordsHandling,
//                "--delimiter", sampleDelimiter,
//                "--quote", sampleQuote,
//                "--newline", sampleNewline,
//                "--column-header",
//                "--columns", sampleColumns,
//                "--column-types", sampleColumnTypes,
//                "--exclude-columns", sampleExcludeColumns,
//                "--only-columns", sampleOnlyColumns,
//                "--prepare-parallel", samplePrepareParallel,
//        };
//        actualOpts.setOptions(args);
//
//        // TODO
//    }

    public void assertOptionEquals(String expectedName, BulkImportOptions actual)
            throws Exception {
        OptionSet set = actual.getOptions();
        assertTrue(set.has(expectedName));
        assertFalse(set.hasArgument(expectedName));
    }

    public void assertOptionEquals(String expectedName, String expectedArg,
            BulkImportOptions actual) throws Exception {
        assertOptionWithRequiredArgEquals(expectedName, actual);
        assertEquals(expectedArg, actual.getOptions().valueOf(expectedName));
    }

    public void assertOptionEquals(String expectedName, String[] expectedArgs,
            BulkImportOptions actual) throws Exception {
        assertOptionWithRequiredArgEquals(expectedName, actual);
        @SuppressWarnings("unchecked")
        List<String> args = (List<String>) actual.getOptions().valuesOf(expectedName);
        assertEquals(expectedArgs.length, args.size());
        for (int i = 0; i < expectedArgs.length; i++) {
            assertEquals(expectedArgs[i], args.get(i));
        }
    }

    public void assertOptionWithRequiredArgEquals(String expectedName,
            BulkImportOptions actual) throws Exception {
        OptionSet set = actual.getOptions();
        assertTrue(set.has(expectedName));
        assertTrue(set.hasArgument(expectedName));
    }

}
