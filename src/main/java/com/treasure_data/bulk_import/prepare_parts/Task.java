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