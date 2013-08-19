//
// Treasure Data Bulk-Import Tool in Java
//
// Copyright (C) 2012 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.bulk_import.prepare_parts;

import java.io.File;

import com.treasure_data.bulk_import.BulkImportStatus;
import com.treasure_data.bulk_import.upload_parts.MultiThreadUploadProcessor;
import com.treasure_data.bulk_import.prepare_parts.Task;

public class UploadTask extends Task {
    public String sessionName;

    public UploadTask(String sessionName, String fileName, BulkImportStatus status) {
        super(fileName, status);
        this.sessionName = sessionName;
    }

    @Override
    public void finishHook(String outputFileName) {
        super.finishHook(outputFileName);

        long size = new File(outputFileName).length();
        com.treasure_data.bulk_import.upload_parts.Task task =
                new com.treasure_data.bulk_import.upload_parts.Task(
                sessionName, outputFileName, size, status);
        MultiThreadUploadProcessor.addTask(task);
    }

    @Override
    public String toString() {
        return String.format("prepare_upload_task{file=%s, session=%s}",
                fileName, sessionName);
    }
}