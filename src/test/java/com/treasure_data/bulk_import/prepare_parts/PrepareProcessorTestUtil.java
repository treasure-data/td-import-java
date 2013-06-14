package com.treasure_data.bulk_import.prepare_parts;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;

import org.junit.Ignore;

@Ignore
public class PrepareProcessorTestUtil {

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

    public static PrepareProcessor.Task createErrorTask(int i)
            throws Exception {
        PrepareProcessor.Task t = new PrepareProcessor.Task("file" + i);
        t = spy(t);
        doThrow(new IOException("dummy")).when(t).createInputStream(
                any(PrepareConfig.CompressionType.class));
        return t;
    }
}
