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
package com.treasure_data.bulk_import.upload_parts;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Task implements com.treasure_data.bulk_import.Task {
    private static final String TAG = "__FINISH__";

    static final Task FINISH_TASK = new Task(TAG, TAG, 0);

    String sessName;
    String partName;
    String fileName;
    long size;

    // unit testing
    public boolean isTest = false;
    public byte[] testBinary = null;

    public Task(String sessName, String fileName, long size) {
        this.sessName = sessName;
        int lastSepIndex = fileName.lastIndexOf(File.separatorChar);
        this.partName = fileName.substring(lastSepIndex + 1,
                fileName.length()).replace('.', '_');
        this.fileName = fileName;
        this.size = size;
    }

//    protected InputStream createInputStream() throws IOException {
//        if (!isTest) {
//            return new BufferedInputStream(new FileInputStream(fileName));
//        } else {
//            return new ByteArrayInputStream(testBinary);
//        }
//    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof Task)) {
            return false;
        }

        Task t = (Task) obj;
        return t.sessName.equals(sessName) && t.partName.equals(partName);
    }

    public boolean endTask() {
        return equals(FINISH_TASK);
    }
}