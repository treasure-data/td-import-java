package com.treasure_data.bulk_import.integration;

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

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "csvfile-with-time_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromCSVWithAlasTimeColumn() throws Exception {
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COMPRESSION, "auto");
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);
        props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "timestamp");

        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(INPUT_DIR + "csvfile-with-aliastime.csv");

        Main.prepareParts(args.toArray(new String[0]), props);

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "csvfile-with-aliastime_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromCSVWithTimeFormat() throws Exception {
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COMPRESSION, "auto");
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);
        props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "timeformat");
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEFORMAT, "%Y-%m-%d %H:%M:%S %z");

        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(INPUT_DIR + "csvfile-with-timeformat.csv");

        Main.prepareParts(args.toArray(new String[0]), props);

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "csvfile-with-timeformat_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

}
