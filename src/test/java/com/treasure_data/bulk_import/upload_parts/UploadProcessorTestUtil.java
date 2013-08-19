package com.treasure_data.bulk_import.upload_parts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Ignore;

@Ignore
public class UploadProcessorTestUtil {

    public static Task createTask(int i) {
        return new Task("sess" + i, "file" + i, 32 + i * 32);
    }

    public static void executeTaskNormally(UploadProcessor proc,
            Task task, TaskResult err) {
        assertEquals(task, err.task);
        assertEquals(null, err.error);
    }

    public static void failTask(UploadProcessor proc,
            Task task, TaskResult err) {
        assertEquals(task, err.task);
        assertTrue(err.error instanceof IOException);
    }
}
