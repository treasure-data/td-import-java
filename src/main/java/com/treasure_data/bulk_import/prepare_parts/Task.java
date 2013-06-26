package com.treasure_data.bulk_import.prepare_parts;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Task implements com.treasure_data.bulk_import.Task {
    private static final String TAG = "__PREPARE_FINISH__";
    static final Task FINISH_TASK = new Task(TAG);

    public String fileName;

    // unit testing
    public boolean isTest = false;
    public byte[] testBinary = null;

    public Task(String fileName) {
        this.fileName = fileName;
    }

    public InputStream createInputStream(
            PrepareConfiguration.CompressionType compressionType)
            throws IOException {
        if (!isTest) {
            return compressionType.createInputStream(new FileInputStream(fileName));
        } else {
            return new ByteArrayInputStream(testBinary);
        }
    }

    public void finishHook(String outputFileName) {
        // do nothing
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof Task)) {
            return false;
        }

        Task t = (Task) obj;
        return t.fileName.equals(fileName);
    }

    @Override
    public String toString() {
        return String.format("prepare_task{file=%s}", fileName);
    }

    @Override
    public boolean endTask() {
        return equals(FINISH_TASK);
    }
}