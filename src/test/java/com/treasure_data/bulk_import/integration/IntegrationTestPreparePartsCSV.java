package com.treasure_data.bulk_import.integration;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.Main;

public class IntegrationTestPreparePartsCSV extends PreparePartsIntegrationTestUtil {
    @Before
    public void createResources() throws Exception {
        super.createResources();
    }

    @After
    public void destroyResources() throws Exception {
        super.destroyResources();
    }

    @Test
    public void writeFromCSVWithTimeColumn() throws Exception {
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COMPRESSION, "auto");
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);
        props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNHEADER, "true");

        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(INPUT_DIR + "csvfile-with-time.csv");

        Main.prepareParts(args.toArray(new String[0]), props);

    }

}
