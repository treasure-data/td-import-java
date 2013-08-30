package com.treasure_data.bulk_import.reader;

import static org.mockito.Mockito.spy;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.BulkImportOptions;
import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.prepare.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare.Task;
import com.treasure_data.bulk_import.writer.FileWriterTestUtil;

public class TestSyslogFileReader {

    protected Properties props;
    protected BulkImportOptions options;
    protected PrepareConfiguration conf;
    protected FileWriterTestUtil writer;
    protected FileReader reader;

    @Test
    public void sample() throws Exception {
        props = new Properties();
        props.setProperty(Configuration.BI_PREPARE_PARTS_SAMPLE_ROWSIZE, "1");

        options = new BulkImportOptions();
        options.initPrepareOptionParser(props);
        options.setOptions(new String[] {
                "--column-header",
        });

        conf = PrepareConfiguration.Format.SYSLOG.createPrepareConfiguration();
        conf.configure(props, options);

        writer = new FileWriterTestUtil(conf);
        reader = PrepareConfiguration.Format.SYSLOG.createFileReader(conf, writer);

        Task task = new Task("dummy.txt");
        task = spy(task);
        task.isTest = true;
        task.testBinary =
                ("Jul 01 00:19:00 muga88 muga88(muga88)[1528965344]: muga88\n"
                        + "Jul 27 09:49:38 itbsv1 su(pam_unix)[8061]: session opened for user root by root(uid=0)\n"
                        + "Jul 27 09:49:38 itbsv1 su(pam_unix)[8061]: session opened for user root by root(uid=0)\n").getBytes();

        reader.configure(task);
        writer.setColumnNames(reader.getColumnNames());
        writer.setColumnTypes(reader.getColumnTypes());
        writer.setSkipColumns(reader.getSkipColumns());

        try {
            reader.next();
            writer.clear();

            reader.next();
            writer.clear();

            reader.next();
            writer.clear();
        } finally {
            reader.close();
        }

    }
}
