package com.treasure_data.bulk_import.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.prepare.PrepareConfiguration.Format;

public class TestPrepareParts extends PreparePartsIntegrationTestUtil {
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
        setOptions(Format.CSV.format(), true, null, null, null, null, null);
        preparePartsFromCSVWithTimeColumn();
    }

    @Test
    public void writeFromCSVWithTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.CSV.format(), true, null, null, null, "timestamp", null);
        preparePartsFromCSVWithTimeColumn();
    }

    @Test
    public void writeFromCSVWithTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.CSV.format(), true, null, null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromCSVWithTimeColumn();
    }

    @Test
    public void writeFromCSVWithAlasTimeColumn() throws Exception {
        setOptions(Format.CSV.format(), true, "timestamp", null, null, null, null);
        preparePartsFromCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromCSVWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.CSV.format(), true, "timestamp", null, null, "timestamp", null);
        preparePartsFromCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromCSVWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.CSV.format(), true, "timestamp", null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromCSVWithTimeFormat() throws Exception {
        setOptions(Format.CSV.format(), true, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        preparePartsFromCSVWithTimeFormat();
    }

    @Test
    public void writeFromCSVWithTimeFormatAndExcludeColumns() throws Exception {
        setOptions(Format.CSV.format(), true, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, "timeformat", null);
        preparePartsFromCSVWithTimeFormat();
    }

    @Test
    public void writeFromCSVWithSuggestedTimeFormat() throws Exception {
        setOptions(Format.CSV.format(), true, "suggested-timeformat", null, null, null, null);
        preparePartsFromCSVWithSuggestedTimeFormat();
    }

    @Test
    public void writeFromCSVWithTimeFormatAndOnlyColumns() throws Exception {
        setOptions(Format.CSV.format(), true, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, "string-value,int-value,double-value,time");
        preparePartsFromCSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeColumn() throws Exception {
        setOptions(Format.CSV.format(), false, null, null, "string-value,int-value,double-value,timestamp,time", null, null);
        preparePartsFromHeaderlessCSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.CSV.format(), false, null, null, "string-value,int-value,double-value,timestamp,time", "timestamp", null);
        preparePartsFromHeaderlessCSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.CSV.format(), false, null, null, "string-value,int-value,double-value,timestamp,time", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessCSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithAlasTimeColumn() throws Exception {
        setOptions(Format.CSV.format(), false, "timestamp", null, "string-value,int-value,double-value,timestamp", null, null);
        preparePartsFromHeaderlessCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.CSV.format(), false, "timestamp", null, "string-value,int-value,double-value,timestamp", "timestamp", null);
        preparePartsFromHeaderlessCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.CSV.format(), false, "timestamp", null, "string-value,int-value,double-value,timestamp", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeFormat() throws Exception {
        setOptions(Format.CSV.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, null);
        preparePartsFromHeaderlessCSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeFormatAndExcludeColumns() throws Exception {
        setOptions(Format.CSV.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", "timeformat", null);
        preparePartsFromHeaderlessCSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeFormatAndOnlyColumns() throws Exception {
        setOptions(Format.CSV.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessCSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessCSVWithSuggestedTimeFormat() throws Exception {
        setOptions(Format.CSV.format(), false, "suggested-timeformat", null, "string-value,int-value,double-value,suggested-timeformat", null, null);
        preparePartsFromHeaderlessCSVWithSuggestedTimeFormat();
    }

    @Test
    public void writeFromTSVWithTimeColumn() throws Exception {
        setOptions(Format.TSV.format(), true, null, null, null, null, null);
        preparePartsFromTSVWithTimeColumn();
    }

    @Test
    public void writeFromTSVWithTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.TSV.format(), true, null, null, null, "timestamp", null);
        preparePartsFromTSVWithTimeColumn();
    }

    @Test
    public void writeFromTSVWithTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.TSV.format(), true, null, null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromTSVWithTimeColumn();
    }

    @Test
    public void writeFromTSVWithAlasTimeColumn() throws Exception {
        setOptions(Format.TSV.format(), true, "timestamp", null, null, null, null);
        preparePartsFromTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromTSVWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.TSV.format(), true, "timestamp", null, null, "timestamp", null);
        preparePartsFromTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromTSVWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.TSV.format(), true, "timestamp", null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromTSVWithTimeFormat() throws Exception {
        setOptions(Format.TSV.format(), true, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        preparePartsFromTSVWithTimeFormat();
    }

    @Test
    public void writeFromTSVWithTimeFormatAndExcludeColumns() throws Exception {
        setOptions(Format.TSV.format(), true, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, "timeformat", null);
        preparePartsFromTSVWithTimeFormat();
    }

    @Test
    public void writeFromTSVWithTimeFormatAndOnlyColumns() throws Exception {
        setOptions(Format.TSV.format(), true, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, "string-value,int-value,double-value,time");
        preparePartsFromTSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeColumn() throws Exception {
        setOptions(Format.TSV.format(), false, null, null, "string-value,int-value,double-value,timestamp,time", null, null);
        preparePartsFromHeaderlessTSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.TSV.format(), false, null, null, "string-value,int-value,double-value,timestamp,time", "timestamp", null);
        preparePartsFromHeaderlessTSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.TSV.format(), false, null, null, "string-value,int-value,double-value,timestamp,time", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessTSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithAlasTimeColumn() throws Exception {
        setOptions(Format.TSV.format(), false, "timestamp", null, "string-value,int-value,double-value,timestamp", null, null);
        preparePartsFromHeaderlessTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.TSV.format(), false, "timestamp", null, "string-value,int-value,double-value,timestamp", "timestamp", null);
        preparePartsFromHeaderlessTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.TSV.format(), false, "timestamp", null, "string-value,int-value,double-value,timestamp", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeFormat() throws Exception {
        setOptions(Format.TSV.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, null);
        preparePartsFromHeaderlessTSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeFormatAndExcludeColumns() throws Exception {
        setOptions(Format.TSV.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", "timeformat", null);
        preparePartsFromHeaderlessTSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeFormatAndOnlyColumns() throws Exception {
        setOptions(Format.TSV.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessTSVWithTimeFormat();
    }

//    @Test // TODO
//    public void writeFromSyslog() throws Exception {
//        setProperties(Format.SYSLOG.format(), false, null, null, null, null, null);
//        preparePartsFromSyslog();
//    }

    @Test
    public void writeFromApacheLog() throws Exception {
        setOptions(Format.APACHE.format(), false, null, null, null, null, null);
        preparePartsFromApacheLog();
    }

    @Test
    public void writeFromJSONWithTimeColumn() throws Exception {
        setOptions(Format.JSON.format(), false, null, null, null, null, null);
        preparePartsFromJSONWithTimeColumn();
    }

    @Test
    public void writeFromJSONWithTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.JSON.format(), false, null, null, null, "timestamp", null);
        preparePartsFromJSONWithTimeColumn();
    }

    @Test
    public void writeFromJSONWithTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.JSON.format(), false, null, null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromJSONWithTimeColumn();
    }

    @Test
    public void writeFromJSONWithAlasTimeColumn() throws Exception {
        setOptions(Format.JSON.format(), false, "timestamp", null, null, null, null);
        preparePartsFromJSONWithAlasTimeColumn();
    }

    @Test
    public void writeFromJSONWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.JSON.format(), false, "timestamp", null, null, "timestamp", null);
        preparePartsFromJSONWithAlasTimeColumn();
    }

    @Test
    public void writeFromJSONWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.JSON.format(), false, "timestamp", null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromJSONWithAlasTimeColumn();
    }

    @Test
    public void writeFromJSONWithTimeFormat() throws Exception {
        setOptions(Format.JSON.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        preparePartsFromJSONWithTimeFormat();
    }

    @Test
    public void writeFromJSONWithTimeFormatAndExcludeColumns() throws Exception {
        setOptions(Format.JSON.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, "timeformat", null);
        preparePartsFromJSONWithTimeFormat();
    }

    @Test
    public void writeFromJSONWithTimeFormatAndOnlyColumns() throws Exception {
        setOptions(Format.JSON.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, "string-value,int-value,double-value,time");
        preparePartsFromJSONWithTimeFormat();
    }

    @Test
    public void writeFromMessagePackWithTimeColumn() throws Exception {
        setOptions(Format.MSGPACK.format(), false, null, null, null, null, null);
        preparePartsFromMessagePackWithTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.MSGPACK.format(), false, null, null, null, "timestamp", null);
        preparePartsFromMessagePackWithTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.MSGPACK.format(), false, null, null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromMessagePackWithTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithAlasTimeColumn() throws Exception {
        setOptions(Format.MSGPACK.format(), false, "timestamp", null, null, null, null);
        preparePartsFromMessagePackWithAlasTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setOptions(Format.MSGPACK.format(), false, "timestamp", null, null, "timestamp", null);
        preparePartsFromMessagePackWithAlasTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setOptions(Format.MSGPACK.format(), false, "timestamp", null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromMessagePackWithAlasTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithTimeFormat() throws Exception {
        setOptions(Format.MSGPACK.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        preparePartsFromMessagePackWithTimeFormat();
    }

    @Test
    public void writeFromMessagePackWithTimeFormatAndExcludeColumns() throws Exception {
        setOptions(Format.MSGPACK.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, "timeformat", null);
        preparePartsFromMessagePackWithTimeFormat();
    }

    @Test
    public void writeFromMessagePackWithTimeFormatAndOnlyColumns() throws Exception {
        setOptions(Format.MSGPACK.format(), false, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, "string-value,int-value,double-value,time");
        preparePartsFromMessagePackWithTimeFormat();
    }

}
