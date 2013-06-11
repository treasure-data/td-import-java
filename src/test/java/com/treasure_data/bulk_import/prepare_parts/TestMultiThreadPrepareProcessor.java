package com.treasure_data.bulk_import.prepare_parts;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.Config;

public class TestMultiThreadPrepareProcessor {

    @Test @Ignore
    public void test01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        props.setProperty(Config.BI_PREPARE_PARTS_PARALLEL, "3");

        PrepareConfig conf = new PrepareConfig();
        conf = spy(conf);
        doReturn(PrepareConfig.CompressionType.NONE).when(conf).checkCompressionType(any(String.class));
        doReturn(PrepareConfig.CompressionType.NONE).when(conf).getCompressionType();
        conf.configure(props);

        MultiThreadPrepareProcessor proc = new MultiThreadPrepareProcessor(conf);
        proc.registerWorkers();
        proc.startWorkers();

        for (int i = 0; i < 10; i++) {
            String csvtext = "time,user,age\n" + "1370416181,muga,10\n";
            String fileName = "file" + i;

            PrepareProcessor.Task task = new PrepareProcessor.Task(fileName);
            task.isTest = true;
            task.testText = csvtext;

            MultiThreadPrepareProcessor.addTask(task);
        }

        MultiThreadPrepareProcessor.addFinishTask(conf);
        proc.joinWorkers();
    }
}
