package com.treasure_data.bulk_import.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.Main;

public class TestPrepareParts extends PreparePartsIntegrationTestUtil {
    @Before
    public void createResources() throws Exception {
        super.createResources();
    }

    @After
    public void destroyResources() throws Exception {
        super.destroyResources();
    }

    private void setProperties(String format, String columnHeader,
            String aliasTimeColumn, String timeFormat, String columnNames, String exclude, String only) {
        // format
        if (format != null && !format.isEmpty()) {
            props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, format);
        }

        // output dir
        props.setProperty(Configuration.BI_PREPARE_PARTS_OUTPUTDIR, OUTPUT_DIR);

        // column header
        if (columnHeader != null && !columnHeader.isEmpty()) {
            props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNHEADER, columnHeader);
        }

        // alias time column
        if (aliasTimeColumn != null && !aliasTimeColumn.isEmpty()) {
            props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, aliasTimeColumn);
        }

        // time format
        if (timeFormat != null && !timeFormat.isEmpty()) {
            props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEFORMAT, timeFormat);
        }

        // column names
        if (columnNames != null && !columnNames.isEmpty()) {
            props.setProperty(Configuration.BI_PREPARE_PARTS_COLUMNS, columnNames);
        }

        // exclude columns
        if (exclude != null && !exclude.isEmpty()) {
            props.setProperty(Configuration.BI_PREPARE_PARTS_EXCLUDE_COLUMNS, exclude);
        }

        // only columns
        if (only != null && !only.isEmpty()) {
            props.setProperty(Configuration.BI_PREPARE_PARTS_ONLY_COLUMNS, exclude);
        }
    }

    private void prepareParts(String fileName) throws Exception {
        args.add(Configuration.CMD_PREPARE_PARTS);
        args.add(fileName);

        Main.prepareParts(args.toArray(new String[0]), props);
    }

    @Test
    public void writeFromCSVWithTimeColumn() throws Exception {
        setProperties("csv", "true", null, null, null, null, null);
        prepareParts(INPUT_DIR + "csvfile-with-time.csv");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "csvfile-with-time_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromCSVWithAlasTimeColumn() throws Exception {
        setProperties("csv", "true", "timestamp", null, null, null, null);
        prepareParts(INPUT_DIR + "csvfile-with-aliastime.csv");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "csvfile-with-aliastime_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromCSVWithTimeFormat() throws Exception {
        setProperties("csv", "true", "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        prepareParts(INPUT_DIR + "csvfile-with-timeformat.csv");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "csvfile-with-timeformat_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeColumn() throws Exception {
        setProperties("csv", "false", null, null, "string-value,int-value,double-value,timestamp,time", null, null);
        prepareParts(INPUT_DIR + "headerless-csvfile-with-time.csv");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "headerless-csvfile-with-time_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromHeaderlessCSVWithAlasTimeColumn() throws Exception {
        setProperties("csv", "false", "timestamp", null, "string-value,int-value,double-value,timestamp", null, null);
        prepareParts(INPUT_DIR + "headerless-csvfile-with-aliastime.csv");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "headerless-csvfile-with-aliastime_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeFormat() throws Exception {
        setProperties("csv", "false", "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, null);
        prepareParts(INPUT_DIR + "headerless-csvfile-with-timeformat.csv");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "headerless-csvfile-with-timeformat_csv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromTSVWithTimeColumn() throws Exception {
        setProperties("tsv", "true", null, null, null, null, null);
        prepareParts(INPUT_DIR + "tsvfile-with-time.tsv");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "tsvfile-with-time_tsv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

  @Test
  public void writeFromTSVWithAlasTimeColumn() throws Exception {
      setProperties("tsv", "true", "timestamp", null, null, null, null);
      prepareParts(INPUT_DIR + "tsvfile-with-aliastime.tsv");

      String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
      String dstFileName = OUTPUT_DIR + "tsvfile-with-aliastime_tsv_0.msgpack.gz";
      assertDataEquals(srcFileName, dstFileName);
  }

  @Test
  public void writeFromTSVWithTimeFormat() throws Exception {
      setProperties("tsv", "true", "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
      prepareParts(INPUT_DIR + "tsvfile-with-timeformat.tsv");

      String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
      String dstFileName = OUTPUT_DIR + "tsvfile-with-timeformat_tsv_0.msgpack.gz";
      assertDataEquals(srcFileName, dstFileName);
  }

  @Test
  public void writeFromHeaderlessTSVWithTimeColumn() throws Exception {
      setProperties("tsv", "false", null, null, "string-value,int-value,double-value,timestamp,time", null, null);
      prepareParts(INPUT_DIR + "headerless-tsvfile-with-time.tsv");

      String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
      String dstFileName = OUTPUT_DIR + "headerless-tsvfile-with-time_tsv_0.msgpack.gz";
      assertDataEquals(srcFileName, dstFileName);
  }

    @Test
    public void writeFromHeaderlessTSVWithAlasTimeColumn() throws Exception {
        setProperties("tsv", "false", "timestamp", null, "string-value,int-value,double-value,timestamp", null, null);
        prepareParts(INPUT_DIR + "headerless-tsvfile-with-aliastime.tsv");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "headerless-tsvfile-with-aliastime_tsv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeFormat() throws Exception {
        setProperties("tsv", "false", "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, null);
        prepareParts(INPUT_DIR + "headerless-tsvfile-with-timeformat.tsv");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "headerless-tsvfile-with-timeformat_tsv_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromJSONWithTimeColumn() throws Exception {
        setProperties("json", null, null, null, null, null, null);
        prepareParts(INPUT_DIR + "jsonfile-with-time.json");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "jsonfile-with-time_json_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromJSONWithAlasTimeColumn() throws Exception {
        setProperties("json", null, "timestamp", null, null, null, null);
        prepareParts(INPUT_DIR + "jsonfile-with-aliastime.json");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "jsonfile-with-aliastime_json_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromJSONWithTimeFormat() throws Exception {
        setProperties("json", null, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        prepareParts(INPUT_DIR + "jsonfile-with-timeformat.json");

        Main.prepareParts(args.toArray(new String[0]), props);

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "jsonfile-with-timeformat_json_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromMessagePackWithTimeColumn() throws Exception {
        setProperties("msgpack", null, null, null, null, null, null);
        prepareParts(INPUT_DIR + "msgpackfile-with-time.msgpack");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "msgpackfile-with-time_msgpack_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromMessagePackWithAlasTimeColumn() throws Exception {
        setProperties("msgpack", null, "timestamp", null, null, null, null);
        prepareParts(INPUT_DIR + "msgpackfile-with-aliastime.msgpack");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "msgpackfile-with-aliastime_msgpack_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }

    @Test
    public void writeFromMessagePackWithTimeFormat() throws Exception {
        setProperties("msgpack", null, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        prepareParts(INPUT_DIR + "msgpackfile-with-timeformat.msgpack");

        String srcFileName = INPUT_DIR + "trainingfile-with-time.msgpack.gz";
        String dstFileName = OUTPUT_DIR + "msgpackfile-with-timeformat_msgpack_0.msgpack.gz";
        assertDataEquals(srcFileName, dstFileName);
    }
}
