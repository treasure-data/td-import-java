package com.treasure_data.bulk_import.reader;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.model.ColumnType;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.Task;
import com.treasure_data.bulk_import.writer.FileWriter;
import com.treasure_data.bulk_import.writer.FileWriterTestUtil;

@Ignore
public class FileReaderTestUtil {
    protected long baseTime;

    protected Properties props;
    protected PrepareConfiguration conf;

    protected FileReader reader;
    protected FileWriter writer;

    protected Task task;
    protected String[] columnNames;
    protected ColumnType[] columnTypes;

    protected Random rand = new Random(new Random().nextInt());

    @Before
    public void createResources() throws Exception {
        baseTime = new Date().getTime() / 1000 / 3600 * 3600;

        createProperties();
        createPrepareConfiguration();
        createFileWriter();
        createFileReader();
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
