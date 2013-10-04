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
package com.treasure_data.bulk_import.upload;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;

public class ImportProcessor extends UploadProcessorBase {

    private static final Logger LOG = Logger.getLogger(ImportProcessor.class.getName());

    private static UploadProcessorBase.RetryClient2 retryClient = new UploadProcessorBase.RetryClient2();

    protected TreasureDataClient client;

    public ImportProcessor(TreasureDataClient client, UploadConfigurationBase conf) {
        super(conf);
        this.client = client;
    }

    public TaskResult execute(final UploadTaskBase base) {
        final ImportTask task = (ImportTask) base;
        TaskResult result = new TaskResult();
        result.task = task;

        try {
            System.out.println(String.format("Importing %s (%d bytes)...", task.fileName, task.size));
            LOG.info(String.format("Importing %s (%d bytes) to %s.%s",
                    task.fileName, task.size, task.databaseName, task.tableName));

            long time = System.currentTimeMillis();
            new RetryClient3().retry(result, new Retryable2() {
                @Override
                public void doTry() throws ClientException, IOException {
                    executeUpload(task);
                }
            }, task.databaseName, task.tableName, conf.getRetryCount(),
                    conf.getWaitSec() * 1000);
            time = System.currentTimeMillis() - time;
            task.finishHook(task.fileName);

            LOG.info(String.format(
                    "Imported file %s (%d bytes) to %s.%s (time: %d sec.)", 
                    task.fileName, task.size, task.databaseName, task.tableName, (time / 1000)));
        } catch (IOException e) {
            LOG.severe(e.getMessage());
            result.error = e;
        }
        return result;
    }

    protected void executeUpload(final ImportTask task) throws ClientException, IOException {
        LOG.fine(String.format("Importing file %s (%d bytes) by thread %s",
                task.fileName, task.size, Thread.currentThread().getName()));

        long time = System.currentTimeMillis();
        client.importData(task.databaseName, task.tableName, getData(task));
        time = System.currentTimeMillis() - time;

        LOG.fine(String.format("Imported file %s (%d bytes) by thread %s (time: %d sec.)",
                task.fileName, task.size, Thread.currentThread().getName(), (time / 1000)));
    }

    private byte[] getData(final ImportTask task) throws IOException {
        if (!task.isTest) {
            InputStream fin = null;
            try {
                File f = new File(task.fileName);
                fin = new BufferedInputStream(new FileInputStream(f));
                byte[] bytes = new byte[(int) f.length()];

                byte[] buf = new byte[1024];
                int pos = 0;
                int len;
                while ((len = fin.read(buf)) != -1) {
                    System.arraycopy(buf, 0, bytes, pos, len);
                    pos += len;
                }

                return bytes;
            } finally {
                if (fin != null) {
                    fin.close();
                }
            }
        } else {
            return task.testBinary;
        }
    }

}
