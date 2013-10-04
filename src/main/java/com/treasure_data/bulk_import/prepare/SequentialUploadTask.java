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
package com.treasure_data.bulk_import.prepare;

import java.io.File;

import com.treasure_data.bulk_import.upload.MultiThreadUploadProcessor;
import com.treasure_data.bulk_import.prepare.Task;

public class SequentialUploadTask extends SequentialUploadTaskBase {
    public String sessionName;

    public SequentialUploadTask(String sessionName, String fileName) {
        super(fileName);
        this.sessionName = sessionName;
    }

    public com.treasure_data.bulk_import.upload.UploadTaskBase createNextTask(String outputFileName) {
        long size = new File(outputFileName).length();
        return new com.treasure_data.bulk_import.upload.UploadTask(
                sessionName, outputFileName, size);
    }

    @Override
    public void finishHook(String outputFileName) {
        super.finishHook(outputFileName);
    }

    @Override
    public String toString() {
        return String.format("prepare_upload_task{file=%s, session=%s}",
                fileName, sessionName);
    }
}