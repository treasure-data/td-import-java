package com.treasure_data.bulk_import.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

@Ignore
public class PreparePartsIntegrationTestUtil {

    static final String INPUT_DIR = "./src/test/resources/in/";
    static final String OUTPUT_DIR = "./src/test/resources/out/";

    protected Properties props;
    protected List<String> args;

    @Before
    public void createResources() throws Exception {
        props = new Properties();
        args = new ArrayList<String>();
    }

    @After
    public void destroyResources() throws Exception {
    }

    public void assertDataEquals(String srcFileName, String dstFileName) throws Exception {
        
    }
}
