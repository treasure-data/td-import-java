package com.treasure_data.bulk_import;

import java.util.Properties;

import org.junit.Ignore;

@Ignore
public class BulkImportOptionsTestUtil {

    public static BulkImportOptions createPrepareOptions(Properties props) {
        BulkImportOptions options = new BulkImportOptions();
        options.initPrepareOptionParser(props);
        return options;
    }

    public static BulkImportOptions createUploadOptions(Properties props) {
        BulkImportOptions options = new BulkImportOptions();
        options.initUploadOptionParser(props);
        return options;
    }
}
