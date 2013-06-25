package com.treasure_data.bulk_import.reader;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PrepareProcessor;
import com.treasure_data.bulk_import.prepare_parts.writer.FileWriterTestUtil;
import com.treasure_data.bulk_import.writer.FileWriter;

@Ignore
public class FileReaderTestUtil {

    protected long baseTime;

    protected Properties props;
    protected PrepareConfiguration conf;

    protected FileReader reader;
    protected FileWriter writer;

    protected PrepareProcessor.Task task;

    protected Random rand = new Random(new Random().nextInt());

    @Before
    public void createResources() throws Exception {
        baseTime = new Date().getTime() / 1000 / 3600 * 3600;

        // create properties
        createProperties();

        // create configuration
        createPrepareConfiguration();

        // create writer
        createFileWriter();

        // create reader
        createFileReader();

        // create prepare task
        createTask();
    }

    protected void createProperties() throws Exception {
        props = System.getProperties();
    }

    protected void createPrepareConfiguration() throws Exception {
        conf = new PrepareConfiguration();
        conf.configure(props);
    }

    protected void createFileWriter() throws Exception {
        writer = new FileWriterTestUtil(conf);
    }

    protected void createFileReader() throws Exception {
        // implement it in subclasses
    }

    protected void createTask() throws Exception {
        // implement it in subclasses
    }

    @After
    public void destroyResources() throws Exception {
        destroyFileWriter();

        destroyFileReader();

    }

    protected void destroyFileWriter() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }

    protected void destroyFileReader() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

}
