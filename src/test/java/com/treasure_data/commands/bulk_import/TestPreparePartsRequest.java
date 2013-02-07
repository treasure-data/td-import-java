package com.treasure_data.commands.bulk_import;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;

import org.junit.Test;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.Config;

public class TestPreparePartsRequest {

    @Test
    public void dontPassFileNames() throws Exception {
        String[] fileNames = new String[0];
        PreparePartsRequest req = new PreparePartsRequest();
        req.setFiles(fileNames);
        assertEquals(fileNames.length, req.getFiles().length);
    }

    @Test
    public void receiveMultiFileNames() throws Exception {
        File tmpFile0 = null, tmpFile1 = null;
        try {
            // create tmp files
            tmpFile0 = File.createTempFile("tmp-", ".tmp");
            tmpFile1 = File.createTempFile("tmp-", ".tmp");

            String[] fileNames = new String[] { tmpFile0.getAbsolutePath(),
                    "muga", tmpFile1.getAbsolutePath(), "nishizawa", };

            PreparePartsRequest req = new PreparePartsRequest();
            req.setFiles(fileNames);

            assertEquals(2, req.getFiles().length);
        } finally {
            // delete tmp files
            if (tmpFile0 != null) {
                tmpFile0.delete();
            }
            if (tmpFile1 != null) {
                tmpFile1.delete();
            }
        }
    }

    @Test
    public void receiveNormalOptions() throws Exception {
        {
            Properties props = new Properties();
            props.setProperty(Config.BI_PREPARE_PARTS_COMPRESSION, "gzip");
            props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "sjis");
            props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "timestamp");
            props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
            // TODO #MN implement time format
            props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
            props.setProperty(Config.BI_PREPARE_PARTS_ERROR_RECORD_OUTPUT, "err");
            props.setProperty(Config.BI_PREPARE_PARTS_DRYRUN, "true");
            props.setProperty(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE, "50");
            props.setProperty(Config.BI_PREPARE_PARTS_SAMPLE_HINT_SCORE, "10");
            props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "1024");
            PreparePartsRequest req = new PreparePartsRequest();
            req.setOptions(props);

            assertEquals(PreparePartsRequest.CompressionType.GZIP, req.getCompressionType());
            assertEquals("sjis", req.getEncoding());
            assertEquals("timestamp", req.getAliasTimeColumn());
            assertEquals(12345L, req.getTimeValue());
            assertEquals("out", req.getOutputDirName());
            assertEquals("err", req.getErrorRecordOutputDirName());
            assertEquals(true, req.dryRun());
            assertEquals(50, req.getSampleRowSize());
            assertEquals(10, req.getSampleHintScore());
            assertEquals(1024, req.getSplitSize());
        }
        { // check default values
            Properties props = new Properties();
            props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
            PreparePartsRequest req = new PreparePartsRequest();
            req.setOptions(props);

            assertEquals(PreparePartsRequest.CompressionType.AUTO, req.getCompressionType());
            assertEquals(Config.BI_PREPARE_PARTS_ENCODING_DEFAULTVALUE, req.getEncoding());
            assertEquals(null, req.getAliasTimeColumn());
            assertEquals(-1L, req.getTimeValue());
            // TODO #MN implement time format
            assertEquals("out", req.getOutputDirName());
            assertEquals(null, req.getErrorRecordOutputDirName());
            assertEquals(Boolean.parseBoolean(Config.BI_PREPARE_PARTS_DRYRUN_DEFAULTVALUE), req.dryRun());
            assertEquals(Integer.parseInt(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE_DEFAULTVALUE), req.getSampleRowSize());
            assertEquals(Integer.parseInt(Config.BI_PREPARE_PARTS_SAMPLE_HINT_SCORE_DEFAULTVALUE), req.getSampleHintScore());
            assertEquals(Integer.parseInt(Config.BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE), req.getSplitSize());
        }
    }

    @Test
    public void throwCmdErrorWhenPassInvalidCompressionType() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_COMPRESSION, "muga");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
        PreparePartsRequest req = new PreparePartsRequest();

        try {
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void throwCmdErrorWhenPassInvalidTimeValue() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "muga");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
        PreparePartsRequest req = new PreparePartsRequest();

        try {
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void throwCmdErrorWhenDontPassOutputDir() throws Exception {
        Properties props = new Properties();
        //props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
        PreparePartsRequest req = new PreparePartsRequest();

        try {
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void throwCmdErrorWhenPassInvalidSampleRowSize() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE, "muga");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
        PreparePartsRequest req = new PreparePartsRequest();

        try {
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void throwCmdErrorWhenPassInvalidSampleHintScore() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_SAMPLE_HINT_SCORE, "muga");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
        PreparePartsRequest req = new PreparePartsRequest();

        try {
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void throwCmdErrorWhenPassInvalidSplitSize() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "muga");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
        PreparePartsRequest req = new PreparePartsRequest();

        try {
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }
}
