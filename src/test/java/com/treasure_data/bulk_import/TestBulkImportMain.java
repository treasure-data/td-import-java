package com.treasure_data.bulk_import;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.Configuration;

public class TestBulkImportMain {

    @Test @Ignore
    public void testPrepareParts01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader()
                .getResourceAsStream("treasure-data.properties"));

        List<String> opts = new ArrayList<String>();
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
        opts.add("--time-column");
        opts.add("date_code");
        opts.add("--prepare-parallel");
        opts.add("2");
        opts.add("--column-header");
        List<String> args = new ArrayList<String>();
        args.add("prepare_parts");
        args.add("in/from_SQLServer_to_csv_10000000_v01.csv");
//        args.add("./in/from_SQLServer_to_csv_10_v01.csv");
//        args.add("./in/from_SQLServer_to_csv_10_v02.csv");
//        args.add("./in/from_SQLServer_to_csv_10_v03.csv");
//        args.add("./in/from_SQLServer_to_csv_10_v04.csv");
//        args.add("./in/from_SQLServer_to_csv_10_v05.csv");
//        args.add("./in/from_SQLServer_to_csv_10_v06.csv");
//        args.add("./in/from_SQLServer_to_csv_10_v07.csv");
//        args.add("./in/from_SQLServer_to_csv_10_v08.csv");
//        args.add("./in/from_SQLServer_to_csv_10_v09.csv");
//        args.add("./in/TE_JNL_ITM_shiftJIS.csv");

        args.addAll(opts);

        BulkImportMain.prepare(args.toArray(new String[0]), props);
    }

//    @Test @Ignore
//    public void testPrepareParts02() throws Exception {
//        Properties props = System.getProperties();
//        props.load(this.getClass().getClassLoader()
//                .getResourceAsStream("treasure-data.properties"));
//
//        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "date_code");
//        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
//        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
//        //props.setProperty(Configuration.BI_PREPARE_PARTS_PARALLEL, "2");
//        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "mysql");
//        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_CONNECTION_URL, props.getProperty("mysql.test.url"));
//        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_USER, props.getProperty("mysql.test.user"));
//        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_PASSWORD, props.getProperty("mysql.test.password"));
//        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_TABLE, props.getProperty("mysql.test.table"));
//        final String[] args = new String[] {
//                "prepare_parts",
//                "mugatbl"
//        };
//
//        BulkImportMain.prepareParts(args, props);
//    }

    @Test
    public void testUploadParts01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader()
                .getResourceAsStream("treasure-data.properties"));

        List<String> opts = new ArrayList<String>();
        //props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "date_code");
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        opts.add("--parallel");
        opts.add("3");

        List<String> args = new ArrayList<String>();
        args.add("upload_parts");
        args.add("mugasess");
        //args.add("out/from_SQLServer_to_csv_10000000_v01_csv_0.msgpack.gz");
        args.add("out/from_SQLServer_to_csv_10_v01_csv_0.msgpack.gz");
        args.add("out/from_SQLServer_to_csv_10_v02_csv_0.msgpack.gz");

        args.addAll(opts);

        BulkImportMain.upload(args.toArray(new String[0]), props);
    }

    @Test @Ignore
    public void testPrepareUploadParts01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader()
                .getResourceAsStream("treasure-data.properties"));

        List<String> opts = new ArrayList<String>();
        opts.add("--column-header");
        opts.add("--time-column");
        opts.add("date_code");
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
        opts.add("--prepare-parallel");
        opts.add("8");

        List<String> args = new ArrayList<String>();
        args.add("upload_parts");
        args.add("mugasess");
        args.add("./in/from_SQLServer_to_csv_10_v01.csv");
        args.add("./in/from_SQLServer_to_csv_10_v02.csv");

        args.addAll(opts);

        BulkImportMain.upload(args.toArray(new String[0]), props);
    }

//    @Test @Ignore
//    public void testPrepareUploadParts02() throws Exception {
//        Properties props = System.getProperties();
//        props.load(this.getClass().getClassLoader()
//                .getResourceAsStream("treasure-data.properties"));
//
//        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "date_code");
//        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
//        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
//        //props.setProperty(Configuration.BI_PREPARE_PARTS_PARALLEL, "2");
//        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "mysql");
//        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_CONNECTION_URL, props.getProperty("mysql.test.url"));
//        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_USER, props.getProperty("mysql.test.user"));
//        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_PASSWORD, props.getProperty("mysql.test.password"));
//        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_TABLE, props.getProperty("mysql.test.table"));
//        final String[] args = new String[] {
//                "upload_parts",
//                "mugasess",
//                "mugatbl"
//        };
//
//        BulkImportMain.prepareAndUploadParts(args, props);
//    }
}
