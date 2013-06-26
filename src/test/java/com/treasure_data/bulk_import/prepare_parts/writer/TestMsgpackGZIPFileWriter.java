package com.treasure_data.bulk_import.prepare_parts.writer;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.writer.FileWriter;

public class TestMsgpackGZIPFileWriter {

    protected Properties props;

    protected PrepareConfiguration conf;
    protected FileWriter writer;

    @Before
    public void createResources() throws Exception {
        // create system properties
        props = System.getProperties();
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTFORMAT,
                Configuration.BI_PREPARE_PARTS_OUTPUTFORMAT_DEFAULTVALUE); // msgpackgz

        // create configuration
        conf = new PrepareConfiguration();
        conf.configure(props);

        // create writer
        writer = conf.getOutputFormat().createFileWriter(conf);

    }

    @After
    public void destroyResources() throws Exception {
    }

    @Test
    public void dummy() throws Exception {
        assertTrue(true);
    }
}
