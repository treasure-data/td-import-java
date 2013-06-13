package com.treasure_data.bulk_import.upload_parts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.bulk_import.Config;
import com.treasure_data.client.ClientException;

public class TestMultiThreadUploadProcessor {

    @Test @Ignore
    public void test01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        UploadConfig conf = new UploadConfig();
        conf.configure(props);
        MultiThreadUploadProcessor proc = new MultiThreadUploadProcessor(conf);
        proc.registerWorkers();
        proc.startWorkers();

        for (int i = 0; i < 10; i++) {
            byte[] bytes = ("muga" + i).getBytes();
            String sessName = "mugasess";
            String fileName = "file" + i;
            long size = bytes.length;

            UploadProcessor.Task task = new UploadProcessor.Task(sessName, fileName, size);
            task = spy(task);
            doReturn(new ByteArrayInputStream(bytes)).when(task).createInputStream();
            MultiThreadUploadProcessor.addTask(task);
        }

        MultiThreadUploadProcessor.addFinishTask(conf);
        proc.joinWorkers();
    }

    private UploadConfig conf;
    private MultiThreadUploadProcessor proc;

    Random rand = new Random(new Random().nextInt());

    private int numTasks;
    private int numWorkers;

    @Before
    public void createResources() throws Exception {
        numWorkers = (rand.nextInt(100) % 8) + 1;
        numTasks = rand.nextInt(100);

        Properties props = System.getProperties();
        props.setProperty(Config.BI_UPLOAD_PARTS_PARALLEL, "" + numWorkers);

        // create upload config
        conf = new UploadConfig();
        conf.configure(props);

        // create multi-thread upload processor
        proc = new MultiThreadUploadProcessor(conf);
    }

    @After
    public void destroyResources() throws Exception {
    }

    @Test
    public void dontGetErrorsWhenWorkersWorkNormally() throws Exception {
        for (int i = 0; i < numWorkers; i++) {
            UploadProcessor child = spy(new UploadProcessor(null, conf));
            doNothing().when(child).executeUpload(any(UploadProcessor.Task.class));
            proc.addWorker(new MultiThreadUploadProcessor.Worker(proc, child));
        }
        proc.startWorkers();

        for (int i = 0; i < numTasks; i++) {
            MultiThreadUploadProcessor.addTask(UploadProcessorTestUtil.createTask(i));
        }

        MultiThreadUploadProcessor.addFinishTask(conf);
        proc.joinWorkers();

        assertEquals(0, proc.getErrors().size());
    }

    @Test
    public void getErrorsWhenWorkersThrowIOError() throws Exception {
        for (int i = 0; i < numWorkers; i++) {
            UploadProcessor child = spy(new UploadProcessor(null, conf));
            doThrow(new IOException("dummy")).when(child).executeUpload(any(UploadProcessor.Task.class));
            proc.addWorker(new MultiThreadUploadProcessor.Worker(proc, child));
        }
        proc.startWorkers();

        for (int i = 0; i < numTasks; i++) {
            MultiThreadUploadProcessor.addTask(UploadProcessorTestUtil.createTask(i));
        }

        MultiThreadUploadProcessor.addFinishTask(conf);
        proc.joinWorkers();

        assertEquals(numTasks, proc.getErrors().size());
        for (UploadProcessor.ErrorInfo err : proc.getErrors()) {
            assertTrue(err.error instanceof IOException);
        }
    }

    @Test
    public void getErrorsWhenWorkersThrowClientError() throws Exception {
        for (int i = 0; i < numWorkers; i++) {
            UploadProcessor child = spy(new UploadProcessor(null, conf));
            doThrow(new ClientException("dummy")).when(child).executeUpload(any(UploadProcessor.Task.class));
            proc.addWorker(new MultiThreadUploadProcessor.Worker(proc, child));
        }
        proc.startWorkers();

        for (int i = 0; i < numTasks; i++) {
            MultiThreadUploadProcessor.addTask(UploadProcessorTestUtil.createTask(i));
        }

        MultiThreadUploadProcessor.addFinishTask(conf);
        proc.joinWorkers();

        assertEquals(numTasks, proc.getErrors().size());
        for (UploadProcessor.ErrorInfo err : proc.getErrors()) {
            assertTrue(err.error instanceof IOException);
        }
    }
}
