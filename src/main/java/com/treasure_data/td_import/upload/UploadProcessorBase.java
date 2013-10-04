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
package com.treasure_data.td_import.upload;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Logger;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.client.bulkimport.BulkImportClient;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.TableSummary;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.SessionSummary;

public class UploadProcessorBase {

    static interface Retryable2 {
        void doTry() throws ClientException, IOException;
    }

    static class RetryClient2 {
        public void retry(Retryable2 r, String sessionName,
                int retryCount, long waitSec) throws IOException {
            ClientException firstException = null;
            int count = 0;
            while (true) {
                try {
                    r.doTry();
                    if (count > 0) {
                        String msg = String.format("Retry succeeded. %s", sessionName);
                        System.out.println(msg);
                        LOG.warning(msg);
                    }
                    break;
                } catch (ClientException e) {
                    if (firstException == null) {
                        firstException = e;
                    }
                    String msg = String.format("ClientError occurred. the cause is %s. %s",
                            e.getMessage(), sessionName);
                    System.out.println(msg);
                    LOG.warning(msg);
                    if (count >= retryCount) {
                        String msg2 = String.format("Retry count exceeded limit. %s",
                                sessionName);
                        System.out.println(msg);
                        LOG.warning(msg2);
                        throw new IOException("Retry failed", firstException);
                    } else {
                        count++;
                        String msg2 = String.format("Retrying. %s", sessionName);
                        System.out.println(msg2);
                        LOG.warning(msg2);
                        try {
                            Thread.sleep(waitSec);
                        } catch (InterruptedException ex) { // ignore
                        }
                    }
                }
            }
        }
    }

    static class RetryClient3 {
        public void retry(TaskResult result, Retryable2 r, String sessionName, String partID,
                int retryCount, long waitSec) throws IOException {
            ClientException firstException = null;
            int count = 0;
            try {
                while (true) {
                    try {
                        r.doTry();
                        if (count > 0) {
                            LOG.warning(String.format("Retry succeeded. %s.%s",
                                    sessionName, partID));
                        }
                        break;
                    } catch (ClientException e) {
                        if (firstException == null) {
                            firstException = e;
                        }
                        LOG.warning(String.format(
                                "ClientError occurred. the cause is %s. %s.%s",
                                e.getMessage(), sessionName, partID));
                        if (count >= retryCount) {
                            LOG.warning(String.format(
                                    "Retry count exceeded limit. %s.%s",
                                    sessionName, partID));
                            throw new IOException("Retry failed", firstException);
                        } else {
                            count++;
                            LOG.warning(String.format("Retrying. %s.%s",
                                    sessionName, partID));
                            try {
                                Thread.sleep(waitSec);
                            } catch (InterruptedException ex) { // ignore
                            }
                        }
                    }
                }
            } finally {
                result.retryCount = count;
            }
        }
    }

    private static final Logger LOG = Logger.getLogger(UploadProcessorBase.class.getName());

    protected UploadConfigurationBase conf;

    public UploadProcessorBase(UploadConfigurationBase conf) {
        this.conf = conf;
    }

    public TaskResult execute(final UploadTaskBase base) {
        throw new UnsupportedOperationException();
    }
}
