package com.treasure_data.bulk_import;

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

        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "date_code");
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
        props.setProperty(Configuration.BI_PREPARE_PARTS_PARALLEL, "2");
        final String[] args = new String[] {
                "prepare_parts",
                "in/from_SQLServer_to_csv_10000000_v01.csv",
                //"./in/sample.csv",
                //"./in/sample2.csv", // for time-value
//                "./in/TE_JNL_ITM_shiftJIS.csv",
//                "./in/from_SQLServer_to_csv_10_v01.csv",
//                "./in/from_SQLServer_to_csv_10_v02.csv",
//                "./in/from_SQLServer_to_csv_10_v03.csv",
//                "./in/from_SQLServer_to_csv_10_v04.csv",
//                "./in/from_SQLServer_to_csv_10_v05.csv",
//                "./in/from_SQLServer_to_csv_10_v06.csv",
//                "./in/from_SQLServer_to_csv_10_v07.csv",
//                "./in/from_SQLServer_to_csv_10_v08.csv",
//                "./in/from_SQLServer_to_csv_10_v09.csv",
        };

        BulkImportMain.prepareParts(args, props);
    }

    @Test @Ignore
    public void testPrepareParts02() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader()
                .getResourceAsStream("treasure-data.properties"));

        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "date_code");
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_PARALLEL, "2");
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "mysql");
        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_CONNECTION_URL, props.getProperty("mysql.test.url"));
        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_USER, props.getProperty("mysql.test.user"));
        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_PASSWORD, props.getProperty("mysql.test.password"));
        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_TABLE, props.getProperty("mysql.test.table"));
        final String[] args = new String[] {
                "prepare_parts",
                "mugatbl"
        };

        BulkImportMain.prepareParts(args, props);
    }

    @Test @Ignore
    public void testUploadParts01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader()
                .getResourceAsStream("treasure-data.properties"));

        //props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "date_code");
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
        props.setProperty(Configuration.BI_UPLOAD_PARTS_PARALLEL, "1");
        props.setProperty(Configuration.BI_UPLOAD_PARTS_AUTO_PERFORM, "false");
        final String[] args = new String[] {
                "upload_parts",
                "mugasess",
//                "./in/TE_JNL_ITM_shiftJIS.csv",
                //"./in/from_SQLServer_to_csv_10_v01.csv",
                "./out/from_SQLServer_to_csv_10000000_v01_csv_0.msgpack.gz",
//                "./in/from_SQLServer_to_csv_10_v02.csv",
//                "./in/from_SQLServer_to_csv_10_v03.csv",
//                "./in/from_SQLServer_to_csv_10_v04.csv",
//                "./in/from_SQLServer_to_csv_10_v05.csv",
//                "./in/from_SQLServer_to_csv_10_v06.csv",
//                "./in/from_SQLServer_to_csv_10_v07.csv",
//                "./in/from_SQLServer_to_csv_10_v08.csv",
//                "./in/from_SQLServer_to_csv_10_v09.csv",
        };

        BulkImportMain.uploadParts(args, props);
    }

    @Test @Ignore
    public void testPrepareUploadParts01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader()
                .getResourceAsStream("treasure-data.properties"));

        props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "date_code");
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
        props.setProperty(Configuration.BI_PREPARE_PARTS_PARALLEL, "8");
        props.setProperty(Configuration.BI_UPLOAD_PARTS_AUTO_PERFORM, "false");
        final String[] args = new String[] {
                "upload_parts",
                "mugasess",
                //"in/from_SQLServer_to_csv_10000000_v01.csv",
                //"./in/sample.csv",
                //"./in/sample2.csv", // for time-value
//                "./in/TE_JNL_ITM_shiftJIS.csv",
                "./in/from_SQLServer_to_csv_10_v01.csv",
                "./in/from_SQLServer_to_csv_10_v02.csv",
//                "./in/from_SQLServer_to_csv_10_v03.csv",
//                "./in/from_SQLServer_to_csv_10_v04.csv",
//                "./in/from_SQLServer_to_csv_10_v05.csv",
//                "./in/from_SQLServer_to_csv_10_v06.csv",
//                "./in/from_SQLServer_to_csv_10_v07.csv",
//                "./in/from_SQLServer_to_csv_10_v08.csv",
//                "./in/from_SQLServer_to_csv_10_v09.csv",
        };

        BulkImportMain.prepareAndUploadParts(args, props);
    }

    @Test @Ignore
    public void testPrepareUploadParts02() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader()
                .getResourceAsStream("treasure-data.properties"));

        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMECOLUMN, "date_code");
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_PARALLEL, "2");
        props.setProperty(Configuration.BI_PREPARE_PARTS_FORMAT, "mysql");
        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_CONNECTION_URL, props.getProperty("mysql.test.url"));
        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_USER, props.getProperty("mysql.test.user"));
        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_PASSWORD, props.getProperty("mysql.test.password"));
        props.setProperty(Configuration.BI_PREPARE_PARTS_JDBC_TABLE, props.getProperty("mysql.test.table"));
        final String[] args = new String[] {
                "upload_parts",
                "mugasess",
                "mugatbl"
        };

        BulkImportMain.prepareAndUploadParts(args, props);
    }
}
