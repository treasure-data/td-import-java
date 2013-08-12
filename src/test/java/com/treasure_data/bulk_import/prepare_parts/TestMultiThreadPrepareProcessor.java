package com.treasure_data.bulk_import.prepare_parts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.BulkImportOptions;
import com.treasure_data.bulk_import.BulkImportOptionsTestUtil;
import com.treasure_data.bulk_import.Configuration;

public class TestMultiThreadPrepareProcessor {

    @Test @Ignore
    public void test01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        BulkImportOptions options = BulkImportOptionsTestUtil.createPrepareOptions(props);
        options.setOptions(new String[] { "--prepare-parallel", "3" });

        CSVPrepareConfiguration conf = new CSVPrepareConfiguration();
        conf = spy(conf);
        doReturn(PrepareConfiguration.CompressionType.NONE).when(conf).checkCompressionType(any(String.class));
        doReturn(PrepareConfiguration.CompressionType.NONE).when(conf).getCompressionType();
        conf.configure(props, options);

        MultiThreadPrepareProcessor proc = new MultiThreadPrepareProcessor(conf);
        proc.registerWorkers();
        proc.startWorkers();

        for (int i = 0; i < 10; i++) {
            String csvtext = "time,user,age\n" + "1370416181,muga,10\n";
            String fileName = "file" + i;

            Task task = new Task(fileName);
            task.isTest = true;
            task.testBinary = csvtext.getBytes();

            MultiThreadPrepareProcessor.addTask(task);
        }

        MultiThreadPrepareProcessor.addFinishTask(conf);
        proc.joinWorkers();
    }

    private Properties props;
    protected BulkImportOptions options;
    private CSVPrepareConfiguration conf;
    private MultiThreadPrepareProcessor proc;

    Random rand = new Random(new Random().nextInt());

    private int numTasks;
    private int numWorkers;
    private int numRows;

    @Before
    public void createResources() throws Exception {
        numWorkers = (rand.nextInt(100) % 8) + 1;
        numTasks = rand.nextInt(100);
        numRows = rand.nextInt(100);

        props = System.getProperties();

        options = new BulkImportOptions();
        options.initPrepareOptionParser(props);
        options.setOptions(new String[] {
                "--prepare-parallel",
                "" + numWorkers,
                "--column-header" });

        // create prepare config
        conf = new CSVPrepareConfiguration();
        conf.configure(props, options);

        // create multi-thread prepare processor
        proc = new MultiThreadPrepareProcessor(conf);
    }

    @After
    public void destroyResources() throws Exception {
    }

    @Test
    public void dontGetErrorsWhenWorkersWorkNormally() throws Exception {
        for (int i = 0; i < numWorkers; i++) {
            PrepareProcessor child = spy(new PrepareProcessor(conf));
            proc.addWorker(new MultiThreadPrepareProcessor.Worker(proc, child));
        }
        proc.startWorkers();

        for (int i = 0; i < numTasks; i++) {
            MultiThreadPrepareProcessor.addTask(PrepareProcessorTestUtil
                    .createTask(i, numRows));
        }

        MultiThreadPrepareProcessor.addFinishTask(conf);
        proc.joinWorkers();

        assertEquals(0, proc.getErrors().size());
    }

    @Test
    public void getErrorsWhenWorkersThrowIOError() throws Exception {
        for (int i = 0; i < numWorkers; i++) {
            PrepareProcessor child = spy(new PrepareProcessor(conf));
            proc.addWorker(new MultiThreadPrepareProcessor.Worker(proc, child));
        }
        proc.startWorkers();

        for (int i = 0; i < numTasks; i++) {
            MultiThreadPrepareProcessor.addTask(PrepareProcessorTestUtil
                    .createErrorTask(i));
        }

        MultiThreadPrepareProcessor.addFinishTask(conf);
        proc.joinWorkers();

        assertEquals(numTasks, proc.getErrors().size());
        for (com.treasure_data.bulk_import.ErrorInfo err : proc.getErrors()) {
            assertTrue(err.error instanceof PreparePartsException);
        }
    }
}
