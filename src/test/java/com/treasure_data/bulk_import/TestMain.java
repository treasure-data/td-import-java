package com.treasure_data.bulk_import;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.upload_parts.MultiThreadUploadProcessor;
import com.treasure_data.bulk_import.upload_parts.UploadConfig;
import com.treasure_data.bulk_import.upload_parts.UploadProcessor;
import com.treasure_data.commands.Config;
import com.treasure_data.tools.BulkImportTool;

public class TestMain {

    @Test @Ignore
    public void testUploadParts01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader()
                .getResourceAsStream("treasure-data.properties"));
        // bulk_import:create mugasess mugadb sesstest
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "./out/");
        props.setProperty(Config.BI_UPLOAD_PARTS_PARALLEL, "2");
        props.setProperty(Config.BI_UPLOAD_PARTS_AUTOPERFORM, "false");
        props.setProperty(Config.BI_UPLOAD_PARTS_AUTOCOMMIT, "false");
        final String[] args = new String[] {
                "upload_parts",
                "mugasess",
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

        Main.uploadParts(args, props);
    }

}
