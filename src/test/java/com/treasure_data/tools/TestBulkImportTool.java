package com.treasure_data.tools;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.commands.Config;

public class TestBulkImportTool {

    @Test @Ignore
    public void testSample() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "time,name,price");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,string,string");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "time");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "./out/");
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");

        final String[] args = new String[] {
                "prepare_parts",
                "./in/test01.csv",
                "./in/test02.csv",
        };
        BulkImportTool.prepareParts(args, props);
    }

    @Test @Ignore
    public void testSample2() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        //props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "date_code,customer_code,product_code,employee_code,pay_method_code,credit_company_code,amount_of_sales,total_sales,original_price,discount_amount,card_point,motivate_code,delete_flag");
        //props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "time,name,count");
        //props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,string,string,string,string,string,string,string,string,string,string,string,string");
        //props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "long,string,int");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "long,string,string,string,string,string,string,string,string,string,string,string,string");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "date_code");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "./out/");

        final String[] args = new String[] {
                "prepare_parts",
                "./from_SQLServer_to_csv_10.csv",
                //"./in/test01.csv",
                //"./in/test02.csv",
        };
        BulkImportTool.prepareParts(args, props);
    }

    @Test @Ignore
    public void testSample4() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNHEADER, "true");
        //props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "date_code,customer_code,product_code,employee_code,pay_method_code,credit_company_code,amount_of_sales,total_sales,original_price,discount_amount,card_point,motivate_code,delete_flag");
        //props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,string,string,string,string,string,string,string,string,string,string,string,string");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,string,string,string,string,string,string,string,string,string,string,string,string");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "1358069100");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "./out/");
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");

        final String[] args = new String[] {
                "prepare_parts",
                "./in/from_SQLServer_to_csv_10_v01.csv",
                "./in/from_SQLServer_to_csv_10_v02.csv",
                "./in/from_SQLServer_to_csv_10_v03.csv",
                "./in/from_SQLServer_to_csv_10_v04.csv",
                "./in/from_SQLServer_to_csv_10_v05.csv",
                "./in/from_SQLServer_to_csv_10_v06.csv",
                "./in/from_SQLServer_to_csv_10_v07.csv",
                "./in/from_SQLServer_to_csv_10_v08.csv",
                "./in/from_SQLServer_to_csv_10_v09.csv",
        };
        BulkImportTool.prepareParts(args, props);
    }

    @Test @Ignore
    public void testSample3() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "date_code,customer_code,product_code,employee_code,pay_method_code,credit_company_code,amount_of_sales,total_sales,original_price,discount_amount,card_point,motivate_code,delete_flag");
        //props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,string,string,string,string,string,string,string,string,string,string,string,string");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES, "string,string,string,string,string,string,string,string,string,string,string,string,string");
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "1358069100");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "./out/");
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + 1024 * 100);

        final String[] args = new String[] {
                "prepare_parts",
                "./from_SQLServer_to_csv_10000000.csv",
        };
        BulkImportTool.prepareParts(args, props);
    }
}
