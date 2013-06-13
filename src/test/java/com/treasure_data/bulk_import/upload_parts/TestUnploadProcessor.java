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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.client.bulkimport.BulkImportClient;

public class TestUnploadProcessor {

    @Test @Ignore
    public void test01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient tdclient = new TreasureDataClient(props);
        BulkImportClient client = new BulkImportClient(tdclient);

        UploadConfig conf = new UploadConfig();
        conf.configure(props);
        UploadProcessor proc = new UploadProcessor(client, conf);

        byte[] bytes = "muga".getBytes();
        String sessName = "mugasess";
        String fileName = "file01";
        long size = bytes.length;

        UploadProcessor.Task task = new UploadProcessor.Task(sessName, fileName, size);
        task = spy(task);
        doReturn(new ByteArrayInputStream(bytes)).when(task).createInputStream();
        proc.execute(task);
    }

    private UploadConfig conf;
    private UploadProcessor proc;

    private String sessName;
    private String fileName;
    private long size;

    private UploadProcessor.Task task;

    @Before
    public void createResources() throws Exception {
        // upload config
        conf = new UploadConfig();
        conf.configure(System.getProperties());

        // upload processor
        proc = new UploadProcessor(null, conf);

        sessName = "sess01";
        fileName = "file01";
        size = 10;

        // task
        task = new UploadProcessor.Task(sessName, fileName, size);
    }

    @After
    public void destroyResources() throws Exception {
    }

    @Test
    public void returnNonErrorWhenExecuteMethodWorksNormally() throws Exception {
        // configure mock
        proc = spy(proc);
        doNothing().when(proc).executeUpload(any(UploadProcessor.Task.class));

        // test
        UploadProcessor.ErrorInfo err = proc.execute(task);
        assertEquals(task, err.task);
        assertEquals(null, err.error);
    }

    @Test
    public void returnIOErrorWhenExecuteMethodThrowsIOError() throws Exception {
        // configure mock
        proc = spy(proc);
        doThrow(new IOException("dummy")).when(proc).executeUpload(any(UploadProcessor.Task.class));

        // test
        UploadProcessor.ErrorInfo error = proc.execute(task);
        assertTrue(error.task.equals(task));
        assertTrue(error.error instanceof IOException);
    }

    @Test
    public void returnIOErrorWhenItThrowsClientError() throws Exception {
        // configure mock
        proc = spy(proc);
        doThrow(new ClientException("dummy")).when(proc).executeUpload(any(UploadProcessor.Task.class));

        // test
        UploadProcessor.ErrorInfo error = proc.execute(task);
        assertTrue(error.task.equals(task));
        assertTrue(error.error instanceof IOException);
    }

    @Test
    public void equalsFinishTasks() {
        assertTrue(UploadProcessor.Task.FINISH_TASK.equals(UploadProcessor.Task.FINISH_TASK));
    }
}
