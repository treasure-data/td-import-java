package com.treasure_data.td_import;

import java.util.Properties;

import org.junit.Ignore;

import com.treasure_data.td_import.BulkImportOptions;

@Ignore
public class BulkImportOptionsTestUtil {

    public static BulkImportOptions createPrepareOptions(Properties props) {
        BulkImportOptions options = new BulkImportOptions();
        options.initPrepareOptionParser(props);
        return options;
    }

    public static BulkImportOptions createPrepareOptions(Properties props, String[] args) {
        BulkImportOptions options = createPrepareOptions(props);
        options.setOptions(args);
        return options;
    }

    public static BulkImportOptions createUploadOptions(Properties props) {
        BulkImportOptions options = new BulkImportOptions();
        options.initUploadOptionParser(props);
        return options;
    }

    public static BulkImportOptions createUploadOptions(Properties props, String[] args) {
        BulkImportOptions options = createUploadOptions(props);
        options.setOptions(args);
        return options;
    }
}
