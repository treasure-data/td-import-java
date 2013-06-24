package com.treasure_data.bulk_import.reader;

import org.junit.After;
import org.junit.Before;

public class TestCSVFileReader extends FileReaderTestUtil {

    @Before
    public void createResources() throws Exception {
        super.createResources();

        // create reader
        reader = new CSVFileReader(conf, writer);

    }

    @After
    public void destroyResources() throws Exception {
    }


}
