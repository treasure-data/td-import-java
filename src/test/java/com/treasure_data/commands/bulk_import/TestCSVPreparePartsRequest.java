package com.treasure_data.commands.bulk_import;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.Config;

public class TestCSVPreparePartsRequest {
    @Test
    public void receiveNormalOptions() throws Exception {
        {
            Properties props = new Properties();
            props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
            props.setProperty(Config.BI_PREPARE_PARTS_DELIMITER, ".");
            props.setProperty(Config.BI_PREPARE_PARTS_NEWLINE, "CR");
            props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true"); // required
            props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");
            props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "int,long");
            props.setProperty(Config.BI_PREPARE_PARTS_TYPE_CONVERSION_ERROR, "none");
            props.setProperty(Config.BI_PREPARE_PARTS_EXCLUDE_COLUMNS, "v0");
            props.setProperty(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE, "50");

            CSVPreparePartsRequest req = new CSVPreparePartsRequest();
            req.setFormat(PreparePartsRequest.Format.CSV);
            req.setOptions(props);

            assertEquals('.', req.getDelimiterChar());
            assertEquals(CSVPreparePartsRequest.NewLine.CR, req.getNewline());
            assertEquals(true, req.hasColumnHeader());
            assertArrayEquals(null, req.getColumnNames());
            assertArrayEquals(new String[] { "int", "long" }, req.getColumnTypeHints());
            assertEquals("none", req.getTypeErrorMode());
            assertArrayEquals(new String[] { "v0" }, req.getExcludeColumns());
            assertArrayEquals(new String[] {}, req.getOnlyColumns());
            assertEquals(50, req.getSampleRowSize());
        }
        { // check default values
            Properties props = new Properties();
            props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required

            CSVPreparePartsRequest req = new CSVPreparePartsRequest();
            req.setFormat(PreparePartsRequest.Format.CSV);
            req.setOptions(props);

            assertEquals(Config.BI_PREPARE_PARTS_DELIMITER_CSV_DEFAULTVALUE.charAt(0), req.getDelimiterChar());
            assertEquals(CSVPreparePartsRequest.NewLine.CRLF, req.getNewline());
            assertEquals(true, req.hasColumnHeader());
            assertArrayEquals(null, req.getColumnNames());
            assertArrayEquals(new String[0], req.getColumnTypeHints());
            assertEquals("skip", req.getTypeErrorMode());
            assertArrayEquals(new String[0], req.getExcludeColumns());
            assertArrayEquals(new String[0], req.getOnlyColumns());
            assertEquals(Integer.parseInt(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE_DEFAULTVALUE), req.getSampleRowSize());
        }
        { // check default values 2
            Properties props = new Properties();
            props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
            CSVPreparePartsRequest req = new CSVPreparePartsRequest();
            req.setFormat(PreparePartsRequest.Format.TSV);
            req.setOptions(props);

            assertEquals(Config.BI_PREPARE_PARTS_DELIMITER_TSV_DEFAULTVALUE.charAt(0), req.getDelimiterChar());
        }
    }

    @Test
    public void throwCmdErrorWhenReceiveInvalidNewLine() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
        props.setProperty(Config.BI_PREPARE_PARTS_NEWLINE, "muga");

        CSVPreparePartsRequest req = new CSVPreparePartsRequest();
        req.setFormat(PreparePartsRequest.Format.CSV);
        try {
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void throwCmdErrorWhenReceiveInvalidColumnNames() throws Exception {
        {
            Properties props = new Properties();
            props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
            props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "false"); // required
            props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");

            CSVPreparePartsRequest req = new CSVPreparePartsRequest();
            req.setFormat(PreparePartsRequest.Format.CSV);
            req.setOptions(props);

            assertEquals(false, req.hasColumnHeader());
            assertArrayEquals(new String[] { "v0", "v1" }, req.getColumnNames());
        }
        {
            Properties props = new Properties();
            props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
            props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "false");

            CSVPreparePartsRequest req = new CSVPreparePartsRequest();
            req.setFormat(PreparePartsRequest.Format.CSV);
            try {
                req.setOptions(props);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof CommandException);
            }
        }
        {
            Properties props = new Properties();
            props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
            props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
            props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1");

            CSVPreparePartsRequest req = new CSVPreparePartsRequest();
            req.setFormat(PreparePartsRequest.Format.CSV);
            req.setOptions(props);

            assertEquals(true, req.hasColumnHeader());
            assertArrayEquals(null, req.getColumnNames());
        }
    }

    @Test
    public void throwCmdErrorWhenReceiveInvalidSampleRowSize() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE, "muga");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out"); // required
        CSVPreparePartsRequest req = new CSVPreparePartsRequest();

        try {
            req.setFormat(PreparePartsRequest.Format.CSV);
            req.setOptions(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

}
