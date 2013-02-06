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
    public void passMultiFileNames() throws Exception {
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
    public void passNormalOptions() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));

        PreparePartsRequest req = new PreparePartsRequest();
        req.setOptions(props);
    }

    @Test
    public void passNotSpecifiedColumnHeader() throws Exception {
        Properties props = new Properties();
        //props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        // props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));

        CSVPreparePartsRequest req = new CSVPreparePartsRequest();
        /**
         * it works fine. if column header is not specified, 'columns' option is
         * used.
         */
        req.setFormat("csv");
        req.setOptions(props);
        String[] columnNames = req.getColumnNames();
        assertEquals("v0", columnNames[0]);
        assertEquals("v1", columnNames[1]);
        assertTrue(!req.hasColumnHeader());
    }

    @Test
    public void passNotSpecifiedColumnHeaderAndColumns() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        // props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        // props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));

        CSVPreparePartsRequest req = new CSVPreparePartsRequest();
        try {
            req.setFormat("csv");
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void passNotSpecifiedColumns() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        // props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));

        CSVPreparePartsRequest req = new CSVPreparePartsRequest();
        /**
         * it works fine. if columns is not specified, 'column-header' option is
         * used.
         */
        req.setFormat("csv");
        req.setOptions(props);
        assertTrue(null == req.getColumnNames());
        assertTrue(req.hasColumnHeader());
    }

    @Test
    public void passNotSpecifiedColumnTypes() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        // props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));

        PreparePartsRequest req = new PreparePartsRequest();
        try {
            req.setFormat("csv");
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void passNotSpecifiedOutputDir() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        // props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));

        PreparePartsRequest req = new PreparePartsRequest();
        try {
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void passUserTimeColumn() throws Exception {
        String tc = "user_time";

        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, tc);
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));

        PreparePartsRequest req = new PreparePartsRequest();
        req.setOptions(props);
        assertEquals(tc, req.getAliasTimeColumn());
    }

    @Test
    public void passNotSpecifiedTimeColumn() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        // props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));

        PreparePartsRequest req = new PreparePartsRequest();
        req.setOptions(props);
        /**
         * it works fine.
         */
        assertTrue(null == req.getAliasTimeColumn());
    }

    @Test
    public void passNotSpecifiedTimeValue() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        // props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));

        PreparePartsRequest req = new PreparePartsRequest();
        req.setOptions(props);
        /**
         * it works fine.
         */
        assertEquals(-1, req.getTimeValue());
    }

    @Test
    public void passUserDefinedSplitSize() throws Exception {
        int splitSize = 5 * 1024;
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + splitSize);

        PreparePartsRequest req = new PreparePartsRequest();
        req.setOptions(props);
        /**
         * it works fine.
         */
        assertEquals(splitSize, req.getSplitSize());
    }

    @Test
    public void passNotSpecifiedSplitSize() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "12345");
        // props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 *
        // 1024));

        PreparePartsRequest req = new PreparePartsRequest();
        req.setOptions(props);
        /**
         * it works fine.
         */
        assertEquals(Config.BI_PREPARE_PARTS_SPLIT_SIZE_DEFAULTVALUE,
                "" + req.getSplitSize());
    }
}
