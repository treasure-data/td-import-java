package com.treasure_data.bulk_import.reader;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.writer.FileWriterTestUtil;
import com.treasure_data.bulk_import.writer.FileWriter;

@Ignore
public class FileReaderTestUtil {

    protected Properties props;
    protected PrepareConfiguration conf;
    protected FileWriter writer;

    protected CSVFileReader reader;

    @Before
    public void createResources() throws Exception {
        props = System.getProperties();

        // create configuration
        conf = new PrepareConfiguration();
        conf.configure(props);

        // create writer
        writer = new FileWriterTestUtil(conf);
    }

    @After
    public void destroyResources() throws Exception {
        if (writer != null) {
            writer.close();
        }

        if (reader != null) {
            reader.close();
        }
    }

    @Test
    public void NextCalled() throws Exception {
        
    }

}
