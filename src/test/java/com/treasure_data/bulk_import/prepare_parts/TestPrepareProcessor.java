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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
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

    private Properties props;
    private PrepareConfig conf;
    private PrepareProcessor proc;

    private PrepareProcessor.Task task;
    private PrepareProcessor.ErrorInfo err;

    Random rand = new Random(new Random().nextInt());
    private int numTasks;
    private int numRows;

    @Before
    public void createResources() throws Exception {
        props = System.getProperties();

        // create prepare conf
        conf = new PrepareConfig();
        conf.configure(props);
        conf = spy(conf);
        doReturn(PrepareConfig.CompressionType.NONE).when(conf).checkCompressionType(any(String.class));
        doReturn(PrepareConfig.CompressionType.NONE).when(conf).getCompressionType();

        // create prepare processor
        proc = new PrepareProcessor(conf);

        numRows = rand.nextInt(100);
        numTasks = rand.nextInt(100);
    }

    @After
    public void destroyResources() throws Exception {
    }

    @Test
    public void dontGetErrorWhenExecuteMethodWorksNormally() throws Exception {
        proc = spy(proc);

        for (int i = 0; i < numTasks; i++) {
            task = createTask(i, numRows);
            err = proc.execute(task);
            assertEquals(task, err.task);
            assertEquals(null, err.error);
            assertEquals(numRows, err.redRows);
            assertEquals(numRows, err.writtenRows);
        }
    }

    @Test
    public void getIOErrorWhenExecuteMethodCannotFindCSVFile() throws Exception {
        proc = spy(proc);

        for (int i = 0; i < numTasks; i++) {
            task = new PrepareProcessor.Task("dummyfile" + i);
            err = proc.execute(task);
            assertEquals(task, err.task);
            assertTrue(err.error instanceof FileNotFoundException);
        }
    }

    public static PrepareProcessor.Task createTask(int i, int numRows) {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("time,user,age\n");
        for (int j = 0; j < numRows; j++) {
            sbuf.append(String.format("1370416181,muga%d,%d\n", i, i));
        }

        PrepareProcessor.Task t = new PrepareProcessor.Task("file" + i);
        t.isTest = true;
        t.testText = sbuf.toString();
        return t;
    }
}
