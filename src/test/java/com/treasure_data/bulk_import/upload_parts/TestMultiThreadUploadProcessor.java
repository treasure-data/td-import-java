package com.treasure_data.bulk_import.upload_parts;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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

    @Before
    public void createResources() throws Exception {
        // upload config
        conf = new UploadConfig();
        conf.configure(System.getProperties());

        // multi-thread upload processor
        proc = new MultiThreadUploadProcessor(conf);
    }

    @After
    public void closeResources() throws Exception {
    }

    @Test
    public void dontGotErrorsWhenItWorksNormally() throws Exception {
        int numTasks = 10;

        UploadProcessor child = spy(new UploadProcessor(null, conf));
        doNothing().when(child).execute0(any(UploadProcessor.Task.class));
        proc.addWorker(new MultiThreadUploadProcessor.Worker(proc, child));
        proc.startWorkers();

        for (int i = 0; i < numTasks; i++) {
            MultiThreadUploadProcessor.addTask(new UploadProcessor.Task("sess" + i, "file" + i, 10));
        }

        MultiThreadUploadProcessor.addFinishTask(conf);
        proc.joinWorkers();

        assertEquals(0, proc.getErrors().size());
    }

    @Test
    public void test() throws Exception {
        int numTasks = 10;

        UploadProcessor child = spy(new UploadProcessor(null, conf));
        doThrow(new IOException("")).when(child).execute0(any(UploadProcessor.Task.class));
        proc.addWorker(new MultiThreadUploadProcessor.Worker(proc, child));
        proc.startWorkers();

        for (int i = 0; i < numTasks; i++) {
            MultiThreadUploadProcessor.addTask(new UploadProcessor.Task("sess" + i, "file" + i, 10));
        }

        MultiThreadUploadProcessor.addFinishTask(conf);
        proc.joinWorkers();

        assertEquals(numTasks, proc.getErrors().size());
    }
}
