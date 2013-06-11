package com.treasure_data.bulk_import.prepare_parts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestPrepareProcessor {

    public InputStream foo() {
        String csvtext = "time,user\n" + "1370416181,muga\n";
        byte[] bytes = csvtext.getBytes();
        return new BufferedInputStream(new ByteArrayInputStream(bytes));
    }

    @Test @Ignore
    public void test01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        PrepareConfig conf = new PrepareConfig();
        conf = spy(conf);
        doReturn(PrepareConfig.CompressionType.NONE).when(conf).checkCompressionType(any(String.class));
        doReturn(PrepareConfig.CompressionType.NONE).when(conf).getCompressionType();
        conf.configure(props);
        PrepareProcessor proc = new PrepareProcessor(conf);

        String csvtext = "time,user,age\n" + "1370416181,muga,10\n";
        String fileName = "file01";

        PrepareProcessor.Task task = new PrepareProcessor.Task(fileName);
        task.isTest = true;
        task.testText = csvtext;

        PrepareProcessor.ErrorInfo err = proc.execute(task);
    }
}
