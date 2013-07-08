package com.treasure_data.bulk_import.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.Main;

public class IntegrationTestPrepareParts extends PreparePartsIntegrationTestUtil {
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

    @Test
    public void writeFromHeaderlessCSVWithTimeColumn() throws Exception {
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COMPRESSION, "auto");
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);
        props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNHEADER, "false");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNS, "string-value,int-value,double-value,timestamp,time");

        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(INPUT_DIR + "headerless-csvfile-with-time.csv");

        Main.prepareParts(args.toArray(new String[0]), props);

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "headerless-csvfile-with-time_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromHeaderlessCSVWithAlasTimeColumn() throws Exception {
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COMPRESSION, "auto");
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);
        props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNHEADER, "false");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNS, "string-value,int-value,double-value,timestamp");
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "timestamp");

        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(INPUT_DIR + "headerless-csvfile-with-aliastime.csv");

        Main.prepareParts(args.toArray(new String[0]), props);

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "headerless-csvfile-with-aliastime_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeFormat() throws Exception {
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COMPRESSION, "auto");
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);
        props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNHEADER, "false");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNS, "string-value,int-value,double-value,timeformat");
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "timeformat");
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEFORMAT, "%Y-%m-%d %H:%M:%S %z");

        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(INPUT_DIR + "headerless-csvfile-with-timeformat.csv");

        Main.prepareParts(args.toArray(new String[0]), props);

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "headerless-csvfile-with-timeformat_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromJSONWithTimeColumn() throws Exception {
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "json");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COMPRESSION, "auto");
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);

        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(INPUT_DIR + "jsonfile-with-time.csv");

        Main.prepareParts(args.toArray(new String[0]), props);

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "jsonfile-with-time_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromJSONWithAlasTimeColumn() throws Exception {
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "json");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COMPRESSION, "auto");
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "timestamp");

        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(INPUT_DIR + "jsonfile-with-aliastime.csv");

        Main.prepareParts(args.toArray(new String[0]), props);

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "jsonfile-with-aliastime_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromJSONWithTimeFormat() throws Exception {
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "json");
        props.setProperty(Configuration.BI_PREPARE_PARTS_COMPRESSION, "auto");
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "timeformat");
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEFORMAT, "%Y-%m-%d %H:%M:%S %z");

        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(INPUT_DIR + "jsonfile-with-timeformat.csv");

        Main.prepareParts(args.toArray(new String[0]), props);

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "headerless-csvfile-with-timeformat_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }
}
