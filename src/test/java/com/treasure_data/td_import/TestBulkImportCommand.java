package com.treasure_data.td_import;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

public class TestBulkImportCommand {
    @Test @Ignore
    public void testPrepareParts00() throws Exception {
        Properties props = System.getProperties();

        List<String> opts = new ArrayList<String>();
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
        opts.add("--format");
        opts.add("csv");
        opts.add("--time-column");
        opts.add("CREATE_TIME");
        opts.add("--encoding");
        opts.add("utf-8");
        opts.add("--time-format");
        opts.add("%Y-%m-%d %H:%M:%S");
        opts.add("--columns");
        opts.add("ID,ORDER_ID,MEMBER_ID,STLM_FLAG,LAST_UPDATE,SENDER_DESIGNATION,IS_ENCLOSE_SPEC,CARRIER,SITE,APPLICANT_SEX,IS_CANCELED,ALLOW_STLM_FLAG,APP_TYPE,UNIT_ID,SALES_SLIP_ID,GOODS_ID,VARIATION_NUMBER,IS_WRAPPING,AMOUNT,LAST_UPDATE_UNIT,CREATE_TIME_UNIT,IS_CLOSED_UNIT,CLOSING_TIME,SHIP_DATE,CURRENT_PRICE,CLOSING_PRICE,POSTAGE_B_NAYOSE,POSTAGE_A_NAYOSE,COD_CMS_B_NAYOSE,COD_CMS_A_NAYOSE,GENRE,NAYOSE_ID,NAYOSE_GROUP_ID,NAYOSE_FLAG,IS_CANCELED_UNIT,FOOT_PRINT,WRAPPING_CMS,HACHU_FLG,HACHU_DATE,CLOSED_MAIL_FLG,CLOSED_MAIL_DATE,SHIP_SHIJI_FLG,SHIP_SHIJI_DATE,CHANGE_STATUS,CARRIER_UNIT,PRODUCT_MSTR_CODE1,PRODUCT_MSTR_CODE2,PRODUCT_ID1,PRODUCT_ID2,TEMP_FLAG,NP_GENRE_ID_1,NP_GENRE_ID_2,NP_GENRE_ID_3,NP_GENRE_ID_4,VENDOR_ID,VENDOR_SECTION_ID,DIRECT_DELIVERY,VC_NAYOSE_FLAG,IS_PRECIOUS,IS_PROCESS,OLD_FLAG,CT_YEAR,CT_MONTH,CT_WEEK,CT_DAY,CT_DAYOFWEEK,CT_HOUR,ALLOTMENT_BASIS,ALLOTMENT_POINT,IS_GRANT_A_POINT,POSTAGE_DISCOUNT_RATE,CLOSE_SCHEDULE,SPECIAL_B_NAYOSE,SPECIAL_A_NAYOSE,DELIVERY_TYPE,PRODUCT_CAT_KBN,PRODUCT_CAT_CODE,CREATE_TIME");
        opts.add("--all-string");
        List<String> args = new ArrayList<String>();
        args.add("prepare");
        args.add("in/uni_sales_slip_2017.csv");

        args.addAll(opts);

        new BulkImportCommand(props).doPrepareCommand(args.toArray(new String[0]));
    }

    @Test @Ignore
    public void testPrepareParts01() throws Exception {
        Properties props = System.getProperties();
//        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        List<String> opts = new ArrayList<String>();
        //props.setProperty(Config.BI_PREPARE_PARTS_ENCODING, "Shift_JIS");
        //props.setProperty(Configuration.BI_PREPARE_PARTS_TIMEVALUE, "1370941200");
        opts.add("--time-column");
        opts.add("date_code");
        opts.add("--column-header");
        List<String> args = new ArrayList<String>();
        args.add("prepare");
//        args.add("in/from_SQLServer_to_csv_10000000_v01.csv");
        args.add("./in/from_SQLServer_to_csv_10_v01.csv");
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

        new BulkImportCommand(props).doPrepareCommand(args.toArray(new String[0]));
    }

    @Test @Ignore
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

        new BulkImportCommand(props).doUploadCommand(args.toArray(new String[0]));
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

        new BulkImportCommand(props).doUploadCommand(args.toArray(new String[0]));
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
