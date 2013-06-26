package com.treasure_data.bulk_import.prepare_parts;

import java.io.File;

import com.treasure_data.bulk_import.upload_parts.MultiThreadUploadProcessor;
import com.treasure_data.bulk_import.prepare_parts.Task;

public class UploadTask extends Task {
    public String sessionName;

    public UploadTask(String sessionName, String fileName) {
        super(fileName);
        this.sessionName = sessionName;
    }

    @Override
    public void finishHook(String outputFileName) {
        super.finishHook(outputFileName);

        long size = new File(outputFileName).length();
        com.treasure_data.bulk_import.upload_parts.Task task =
                new com.treasure_data.bulk_import.upload_parts.Task(
                sessionName, outputFileName, size);
        MultiThreadUploadProcessor.addTask(task);
    }

    @Override
    public String toString() {
        return String.format("prepare_upload_task{file=%s, session=%s}",
                fileName, sessionName);
    }
}