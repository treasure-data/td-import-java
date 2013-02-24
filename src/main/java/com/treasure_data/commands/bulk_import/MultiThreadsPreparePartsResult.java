//
// Java Extension to CUI for Treasure Data
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
package com.treasure_data.commands.bulk_import;

public class MultiThreadsPreparePartsResult extends PreparePartsResult {

    public MultiThreadsPreparePartsResult() {
        super();
    }

    public synchronized void addOutputFilePath(String filePath) {
        super.addOutputFilePath(filePath);
        PrepareUploadPartsCommand.uploadTaskQueue.add(
                new PrepareUploadPartsCommand.UploadWorker.Task(filePath));
    }

    public Object clone() {
        return new MultiThreadsPreparePartsResult();
    }

    public synchronized void addFinishTask(int numOfThreads) {
        for (int i = 0; i < numOfThreads; i++) {
            PrepareUploadPartsCommand.uploadTaskQueue
            .add(PrepareUploadPartsCommand.UploadWorker.FINISH_TASK);
        }
    }
}
