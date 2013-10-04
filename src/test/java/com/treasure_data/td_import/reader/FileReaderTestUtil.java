package com.treasure_data.td_import.reader;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import com.treasure_data.td_import.BulkImportOptions;
import com.treasure_data.td_import.model.ColumnType;
import com.treasure_data.td_import.prepare.PrepareConfiguration;
import com.treasure_data.td_import.prepare.Task;
import com.treasure_data.td_import.reader.FileReader;
import com.treasure_data.td_import.writer.FileWriter;
import com.treasure_data.td_import.writer.FileWriterTestUtil;

@Ignore
public class FileReaderTestUtil<T extends PrepareConfiguration> {
    protected long baseTime;

    protected Properties props;
    protected BulkImportOptions options;
    protected T conf;

    protected FileReader<T> reader;
    protected FileWriter writer;

    protected Task task;
    protected String[] columnNames;
    protected ColumnType[] columnTypes;

    protected Random rand = new Random(new Random().nextInt());

    @Before
    public void createResources() throws Exception {
        baseTime = new Date().getTime() / 1000 / 3600 * 3600;

        createProperties();
        createBulkImportOptions();
        createPrepareConfiguration();
        createFileWriter();
        createFileReader();
    }

    protected void createProperties() throws Exception {
        props = System.getProperties();
    }

    protected void createBulkImportOptions() throws Exception {
        options = new BulkImportOptions();
        options.initPrepareOptionParser(props);
    }

    protected void createPrepareConfiguration() throws Exception {
        //conf = new PrepareConfiguration();
        //conf.configure(props);
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
