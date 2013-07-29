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

    @Test
    public void writeFromCSVWithTimeColumn() throws Exception {
        setProperties("csv", "true", null, null, null, null, null);
        preparePartsFromCSVWithTimeColumn();
    }

    @Test
    public void writeFromCSVWithTimeColumnAndExcludeColumns() throws Exception {
        setProperties("csv", "true", null, null, null, "timestamp", null);
        preparePartsFromCSVWithTimeColumn();
    }

    @Test
    public void writeFromCSVWithTimeColumnAndOnlyColumns() throws Exception {
        setProperties("csv", "true", null, null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromCSVWithTimeColumn();
    }

    @Test
    public void writeFromCSVWithAlasTimeColumn() throws Exception {
        setProperties("csv", "true", "timestamp", null, null, null, null);
        preparePartsFromCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromCSVWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setProperties("csv", "true", "timestamp", null, null, "timestamp", null);
        preparePartsFromCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromCSVWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setProperties("csv", "true", "timestamp", null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromCSVWithTimeFormat() throws Exception {
        setProperties("csv", "true", "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        preparePartsFromCSVWithTimeFormat();
    }

    @Test
    public void writeFromCSVWithTimeFormatAndExcludeColumns() throws Exception {
        setProperties("csv", "true", "timeformat", "%Y-%m-%d %H:%M:%S %z", null, "timeformat", null);
        preparePartsFromCSVWithTimeFormat();
    }

    @Test
    public void writeFromCSVWithTimeFormatAndOnlyColumns() throws Exception {
        setProperties("csv", "true", "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, "string-value,int-value,double-value,time");
        preparePartsFromCSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeColumn() throws Exception {
        setProperties("csv", "false", null, null, "string-value,int-value,double-value,timestamp,time", null, null);
        preparePartsFromHeaderlessCSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeColumnAndExcludeColumns() throws Exception {
        setProperties("csv", "false", null, null, "string-value,int-value,double-value,timestamp,time", "timestamp", null);
        preparePartsFromHeaderlessCSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeColumnAndOnlyColumns() throws Exception {
        setProperties("csv", "false", null, null, "string-value,int-value,double-value,timestamp,time", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessCSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithAlasTimeColumn() throws Exception {
        setProperties("csv", "false", "timestamp", null, "string-value,int-value,double-value,timestamp", null, null);
        preparePartsFromHeaderlessCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setProperties("csv", "false", "timestamp", null, "string-value,int-value,double-value,timestamp", "timestamp", null);
        preparePartsFromHeaderlessCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setProperties("csv", "false", "timestamp", null, "string-value,int-value,double-value,timestamp", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessCSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeFormat() throws Exception {
        setProperties("csv", "false", "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, null);
        preparePartsFromHeaderlessCSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeFormatAndExcludeColumns() throws Exception {
        setProperties("csv", "false", "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", "timeformat", null);
        preparePartsFromHeaderlessCSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessCSVWithTimeFormatAndOnlyColumns() throws Exception {
        setProperties("csv", "false", "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessCSVWithTimeFormat();
    }

    @Test
    public void writeFromTSVWithTimeColumn() throws Exception {
        setProperties("tsv", "true", null, null, null, null, null);
        preparePartsFromTSVWithTimeColumn();
    }

    @Test
    public void writeFromTSVWithTimeColumnAndExcludeColumns() throws Exception {
        setProperties("tsv", "true", null, null, null, "timestamp", null);
        preparePartsFromTSVWithTimeColumn();
    }

    @Test
    public void writeFromTSVWithTimeColumnAndOnlyColumns() throws Exception {
        setProperties("tsv", "true", null, null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromTSVWithTimeColumn();
    }

    @Test
    public void writeFromTSVWithAlasTimeColumn() throws Exception {
        setProperties("tsv", "true", "timestamp", null, null, null, null);
        preparePartsFromTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromTSVWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setProperties("tsv", "true", "timestamp", null, null, "timestamp", null);
        preparePartsFromTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromTSVWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setProperties("tsv", "true", "timestamp", null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromTSVWithTimeFormat() throws Exception {
        setProperties("tsv", "true", "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        preparePartsFromTSVWithTimeFormat();
    }

    @Test
    public void writeFromTSVWithTimeFormatAndExcludeColumns() throws Exception {
        setProperties("tsv", "true", "timeformat", "%Y-%m-%d %H:%M:%S %z", null, "timeformat", null);
        preparePartsFromTSVWithTimeFormat();
    }

    @Test
    public void writeFromTSVWithTimeFormatAndOnlyColumns() throws Exception {
        setProperties("tsv", "true", "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, "string-value,int-value,double-value,time");
        preparePartsFromTSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeColumn() throws Exception {
        setProperties("tsv", "false", null, null, "string-value,int-value,double-value,timestamp,time", null, null);
        preparePartsFromHeaderlessTSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeColumnAndExcludeColumns() throws Exception {
        setProperties("tsv", "false", null, null, "string-value,int-value,double-value,timestamp,time", "timestamp", null);
        preparePartsFromHeaderlessTSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeColumnAndOnlyColumns() throws Exception {
        setProperties("tsv", "false", null, null, "string-value,int-value,double-value,timestamp,time", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessTSVWithTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithAlasTimeColumn() throws Exception {
        setProperties("tsv", "false", "timestamp", null, "string-value,int-value,double-value,timestamp", null, null);
        preparePartsFromHeaderlessTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setProperties("tsv", "false", "timestamp", null, "string-value,int-value,double-value,timestamp", "timestamp", null);
        preparePartsFromHeaderlessTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setProperties("tsv", "false", "timestamp", null, "string-value,int-value,double-value,timestamp", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessTSVWithAlasTimeColumn();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeFormat() throws Exception {
        setProperties("tsv", "false", "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, null);
        preparePartsFromHeaderlessTSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeFormatAndExcludeColumns() throws Exception {
        setProperties("tsv", "false", "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", "timeformat", null);
        preparePartsFromHeaderlessTSVWithTimeFormat();
    }

    @Test
    public void writeFromHeaderlessTSVWithTimeFormatAndOnlyColumns() throws Exception {
        setProperties("tsv", "false", "timeformat", "%Y-%m-%d %H:%M:%S %z", "string-value,int-value,double-value,timeformat", null, "string-value,int-value,double-value,time");
        preparePartsFromHeaderlessTSVWithTimeFormat();
    }

    @Test
    public void writeFromJSONWithTimeColumn() throws Exception {
        setProperties("json", null, null, null, null, null, null);
        preparePartsFromJSONWithTimeColumn();
    }

    @Test
    public void writeFromJSONWithTimeColumnAndExcludeColumns() throws Exception {
        setProperties("json", null, null, null, null, "timestamp", null);
        preparePartsFromJSONWithTimeColumn();
    }

    @Test
    public void writeFromJSONWithTimeColumnAndOnlyColumns() throws Exception {
        setProperties("json", null, null, null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromJSONWithTimeColumn();
    }

    @Test
    public void writeFromJSONWithAlasTimeColumn() throws Exception {
        setProperties("json", null, "timestamp", null, null, null, null);
        preparePartsFromJSONWithAlasTimeColumn();
    }

    @Test
    public void writeFromJSONWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setProperties("json", null, "timestamp", null, null, "timestamp", null);
        preparePartsFromJSONWithAlasTimeColumn();
    }

    @Test
    public void writeFromJSONWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setProperties("json", null, "timestamp", null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromJSONWithAlasTimeColumn();
    }

    @Test
    public void writeFromJSONWithTimeFormat() throws Exception {
        setProperties("json", null, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        preparePartsFromJSONWithTimeFormat();
    }

    @Test
    public void writeFromJSONWithTimeFormatAndExcludeColumns() throws Exception {
        setProperties("json", null, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, "timeformat", null);
        preparePartsFromJSONWithTimeFormat();
    }

    @Test
    public void writeFromJSONWithTimeFormatAndOnlyColumns() throws Exception {
        setProperties("json", null, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, "string-value,int-value,double-value,time");
        preparePartsFromJSONWithTimeFormat();
    }

    @Test
    public void writeFromMessagePackWithTimeColumn() throws Exception {
        setProperties("msgpack", null, null, null, null, null, null);
        preparePartsFromMessagePackWithTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithTimeColumnAndExcludeColumns() throws Exception {
        setProperties("msgpack", null, null, null, null, "timestamp", null);
        preparePartsFromMessagePackWithTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithTimeColumnAndOnlyColumns() throws Exception {
        setProperties("msgpack", null, null, null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromMessagePackWithTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithAlasTimeColumn() throws Exception {
        setProperties("msgpack", null, "timestamp", null, null, null, null);
        preparePartsFromMessagePackWithAlasTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithAlasTimeColumnAndExcludeColumns() throws Exception {
        setProperties("msgpack", null, "timestamp", null, null, "timestamp", null);
        preparePartsFromMessagePackWithAlasTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithAlasTimeColumnAndOnlyColumns() throws Exception {
        setProperties("msgpack", null, "timestamp", null, null, null, "string-value,int-value,double-value,time");
        preparePartsFromMessagePackWithAlasTimeColumn();
    }

    @Test
    public void writeFromMessagePackWithTimeFormat() throws Exception {
        setProperties("msgpack", null, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, null);
        preparePartsFromMessagePackWithTimeFormat();
    }

    @Test
    public void writeFromMessagePackWithTimeFormatAndExcludeColumns() throws Exception {
        setProperties("msgpack", null, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, "timeformat", null);
        preparePartsFromMessagePackWithTimeFormat();
    }

    @Test
    public void writeFromMessagePackWithTimeFormatAndOnlyColumns() throws Exception {
        setProperties("msgpack", null, "timeformat", "%Y-%m-%d %H:%M:%S %z", null, null, "string-value,int-value,double-value,time");
        preparePartsFromMessagePackWithTimeFormat();
    }

}
