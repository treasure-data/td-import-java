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

public class UploadProcessor {

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

    private static final Logger LOG = Logger.getLogger(UploadProcessor.class.getName());

    private static SessionSummary summary;
    private static RetryClient2 retryClient = new RetryClient2();

    private BulkImportClient client;
    private UploadConfiguration conf;

    public UploadProcessor(BulkImportClient client, UploadConfiguration conf) {
        this.conf = conf;
        this.client = client;
    }

    public TaskResult execute(final Task task) {
        TaskResult result = new TaskResult();
        result.task = task;

        try {
            System.out.println(String.format("Uploading %s (%d bytes)...", task.fileName, task.size));
            LOG.info(String.format("Uploading %s (%d bytes) to session %s as part %s",
                    task.fileName, task.size, task.sessName, task.partName));

            long time = System.currentTimeMillis();
            new RetryClient3().retry(result, new Retryable2() {
                @Override
                public void doTry() throws ClientException, IOException {
                    executeUpload(task);
                }
            }, task.sessName, task.partName, conf.getRetryCount(),
                    conf.getWaitSec() * 1000);
            time = System.currentTimeMillis() - time;
            task.finishHook(task.fileName);

            LOG.info(String.format(
                    "Uploaded file %s (%d bytes) to session %s as part %s (time: %d sec.)", 
                    task.fileName, task.size, task.sessName, task.partName, (time / 1000)));
        } catch (IOException e) {
            LOG.severe(e.getMessage());
            result.error = e;
        }
        return result;
    }

    protected void executeUpload(final Task task) throws ClientException, IOException {
        LOG.fine(String.format("Uploading file %s (%d bytes) to session %s as part %s by thread %s",
                task.fileName, task.size, task.sessName, task.partName, Thread.currentThread().getName()));

        long time = System.currentTimeMillis();
        Session session = new Session(task.sessName, null, null);
        client.uploadPart(session, task.partName, createInputStream(task),
                (int) task.size);
        time = System.currentTimeMillis() - time;

        LOG.fine(String.format("Uploaded file %s (%d bytes) to session %s as part %s by thread %s (time: %d sec.)",
                task.fileName, task.size, task.sessName, task.partName,
                Thread.currentThread().getName(), (time / 1000)));
    }

    protected InputStream createInputStream(final Task task) throws IOException {
        if (!task.isTest) {
            return new BufferedInputStream(new FileInputStream(task.fileName));
        } else {
            return new ByteArrayInputStream(task.testBinary);
        }
    }

    public static TaskResult processAfterUploading(BulkImportClient client,
            UploadConfiguration conf, String sessName) throws UploadPartsException {
        TaskResult err = null;

        if (!conf.autoPerform()) {
            return new TaskResult();
        }

        // freeze
        err = freezeSession(client, conf, sessName);
        if (err.error != null) {
            return err;
        }

        // perform
        err = performSession(client, conf, sessName);
        if (err.error != null) {
            return err;
        }

        // check session status
        SessionSummary summary = null;
        try {
            summary = showSession(client, conf, sessName);

            StringBuilder sbuf = new StringBuilder();
            sbuf.append(String.format("Show status of bulk import session %s", summary.getName())).append("\n");
            sbuf.append("  Performing job ID : " + summary.getJobID()).append("\n");
            sbuf.append("  Name              : " + summary.getName()).append("\n");
            sbuf.append("  Status            : " + summary.getStatus()).append("\n");

            System.out.println(sbuf.toString());
            LOG.info(sbuf.toString());
        } catch (IOException e) {
            String m = String.format("Session status checking failed: %s", e.getMessage());
            System.out.println(m);
            LOG.severe(m);
            err.error = e;
        }

        if (summary == null) {
            return err;
        }

        // TODO FIXME #MN need log message

        if (!conf.autoCommit()) {
            return new TaskResult();
        }

        // wait performing
        err = waitPerform(client, conf, sessName);
        if (err.error != null) {
            return err;
        }

        // check error of perform
        summary = null;
        try {
            summary = showSession(client, conf, sessName);

            StringBuilder sbuf = new StringBuilder();
            sbuf.append(String.format("Show error records of bulk import session %s", summary.getName())).append("\n");
            sbuf.append("  Performing job ID : " + summary.getJobID()).append("\n");
            sbuf.append("  Valid parts       : " + summary.getValidParts()).append("\n");
            sbuf.append("  Error parts       : " + summary.getErrorParts()).append("\n");
            sbuf.append("  Valid records     : " + summary.getValidRecords()).append("\n");
            sbuf.append("  Error records     : " + summary.getErrorRecords()).append("\n");

            System.out.println(sbuf.toString());
            LOG.info(sbuf.toString());
        } catch (IOException e) {
            String m = String.format("Error records checking failed: %s", e.getMessage());
            System.out.println(m);
            LOG.severe(m);
            err.error = e;
        }

        if (summary == null) {
            return err;
        }

        if (summary.getErrorParts() != 0 || summary.getErrorRecords() != 0) {
            String msg = String.format(
                    "Performing failed: error parts = %d, error records = %d",
                    summary.getErrorParts(), summary.getErrorRecords());
            System.out.println(msg);
            LOG.severe(msg);

            msg = String.format(
                    "Check the status of bulk import session %s with 'td import:show %s' command",
                    summary.getName(), summary.getName());
            System.out.println(msg);
            LOG.severe(msg);
            err.error = new UploadPartsException(msg);
            return err;
        }

        // commit
        err = commitSession(client, conf, sessName);
        if (err.error != null) {
            return err;
        }

        if (!conf.autoDelete()) {
            return new TaskResult();
        }

        // wait committing
        err = waitCommit(client, conf, sessName);
        if (err.error != null) {
            return err;
        }

        return new TaskResult();
    }

    public static SessionSummary showSession(final BulkImportClient client,
            final UploadConfiguration conf, final String sessionName) throws IOException {
        LOG.fine(String.format("Show bulk import session %s", sessionName));

        summary = null;
        try {
            retryClient.retry(new Retryable2(){
                @Override
                public void doTry() throws ClientException, IOException {
                    summary = client.showSession(sessionName);
                }
            }, sessionName, conf.getRetryCount(), conf.getWaitSec());
        } catch (IOException e) {
            LOG.severe(e.getMessage());
            throw e;
        }
        return summary;
    }

    public static TaskResult freezeSession(final BulkImportClient client,
            final UploadConfiguration conf, final String sessionName) {
        String m = String.format("Freeze bulk import session %s", sessionName);
        System.out.println(m);
        LOG.info(m);

        TaskResult err = new TaskResult();
        try {
            retryClient.retry(new Retryable2(){
                @Override
                public void doTry() throws ClientException, IOException {
                    Session session = new Session(sessionName, null, null);
                    client.freezeSession(session);
                }
            }, sessionName, conf.getRetryCount(), conf.getWaitSec());
        } catch (IOException e) {
            m = String.format("Cannot freeze session %s, %s", sessionName, e.getMessage());
            System.out.println(m);
            LOG.severe(m);
            err.error = e;
        }
        return err;
    }

    public static TaskResult performSession(final BulkImportClient client,
            final UploadConfiguration conf, final String sessionName) {
        String m = String.format("Perform bulk import session %s", sessionName);
        System.out.println(m);
        LOG.info(m);

        TaskResult err = new TaskResult();
        try {
            retryClient.retry(new Retryable2(){
                @Override
                public void doTry() throws ClientException, IOException {
                    Session session = new Session(sessionName, null, null);
                    client.performSession(session);
                }
            }, sessionName, conf.getRetryCount(), conf.getWaitSec());
        } catch (IOException e) {
            m = String.format("Cannot perform bulk import session %s, %s", sessionName, e.getMessage());
            System.out.println(m);
            LOG.severe(m);
            err.error = e;
        }
        return err;
    }

    public static TaskResult waitPerform(final BulkImportClient client,
            final UploadConfiguration conf, final String sessionName) throws UploadPartsException {
        String m = String.format("Wait %s bulk import session performing...", sessionName);
        System.out.println(m);
        LOG.info(m);

        TaskResult err = new TaskResult();
        long waitTime = System.currentTimeMillis();
        while (true) {
            try {
                retryClient.retry(new Retryable2(){
                    @Override
                    public void doTry() throws ClientException, IOException {
                        summary = client.showSession(sessionName);
                    }
                }, sessionName, conf.getRetryCount(), conf.getWaitSec());

                if (summary.getStatus().equals("ready")){
                    break;
                } else if (summary.getStatus().equals("uploading")) {
                    throw new IOException("performing failed");
                }

                try {
                    long deltaTime = System.currentTimeMillis() - waitTime;
                    LOG.fine(String.format("Waiting for about %d sec.", (deltaTime / 1000)));
                    Thread.sleep(3 * 1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            } catch (IOException e) {
                m = String.format("Give up waiting %s bulk import session performing, %s", sessionName, e.getMessage());
                System.out.println(m);
                LOG.severe(m);
                err.error = e;
                break;
            }
        }

        return err;
    }

    public static TaskResult commitSession(final BulkImportClient client,
            final UploadConfiguration conf, final String sessionName) throws UploadPartsException {
        String msg = String.format("Commit %s bulk import session", sessionName);
        System.out.println(msg);
        LOG.info(msg);

        TaskResult err = new TaskResult();
        try {
            retryClient.retry(new Retryable2(){
                @Override
                public void doTry() throws ClientException, IOException {
                    Session session = new Session(sessionName, null, null);
                    client.commitSession(session);
                }
            }, sessionName, conf.getRetryCount(), conf.getWaitSec());
        } catch (IOException e) {
            String emsg = String.format("Cannot commit '%s' bulk import session, %s", sessionName, e.getMessage());
            System.out.println(emsg);
            LOG.severe(emsg);
            err.error = e;
        }
        return err;
    }

    public static TaskResult waitCommit(final BulkImportClient client,
            final UploadConfiguration conf, final String sessionName) throws UploadPartsException {
        String m = String.format("Wait %s bulk import session committing...", sessionName);
        System.out.println(m);
        LOG.info(m);

        TaskResult err = new TaskResult();
        long waitTime = System.currentTimeMillis();
        while (true) {
            try {
                retryClient.retry(new Retryable2(){
                    @Override
                    public void doTry() throws ClientException, IOException {
                        summary = client.showSession(sessionName);
                    }
                }, sessionName, conf.getRetryCount(), conf.getWaitSec());

                if (summary.getStatus().equals("committed")){
                    break;
                } else if (summary.getStatus().equals("ready")) {
                    throw new IOException("committing failed");
                }

                try {
                    long deltaTime = System.currentTimeMillis() - waitTime;
                    LOG.fine(String.format("Waiting for about %d sec.", (deltaTime / 1000)));
                    Thread.sleep(3 * 1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            } catch (IOException e) {
                m = String.format("Give up waiting %s bulk import session committing, %s", sessionName, e.getMessage());
                System.out.println(m);
                LOG.severe(m);
                err.error = e;
                break;
            }
        }

        return err;
    }

    public static TaskResult checkDatabase(final TreasureDataClient client, final UploadConfiguration conf,
            final String sessionName, final String databaseName) throws UploadPartsException {
        LOG.info(String.format("Check database %s", databaseName));

        TaskResult err = new TaskResult();
        try {
            retryClient.retry(new Retryable2() {
                @Override
                public void doTry() throws ClientException, IOException {
                    List<DatabaseSummary> dbs = client.listDatabases();
                    boolean exist = false;
                    for (DatabaseSummary db : dbs) {
                        if (db.getName().equals(databaseName)) {
                            exist = true;
                            break;
                        }
                    }

                    if (!exist) {
                        throw new IOException(String.format(
                                "Not found database %s", databaseName));
                    }
                }
            }, sessionName, conf.getRetryCount(), conf.getWaitSec());
        } catch (IOException e) {
            String msg = String.format(
                    "Cannot access database %s, %s. " +
                    "Please check it with 'td database:list'. " +
                    "If it doesn't exist, please create it with 'td database:create %s'.",
                    databaseName, e.getMessage(), databaseName);
            System.out.println(msg);
            LOG.severe(msg);
            err.error = e;
        }

        return err;
    }

    public static TaskResult checkTable(final TreasureDataClient client, final UploadConfiguration conf,
            final String sessionName, final String databaseName, final String tableName) throws UploadPartsException {
        LOG.info(String.format("Check table %s", tableName));

        TaskResult err = new TaskResult();
        try {
            retryClient.retry(new Retryable2() {
                @Override
                public void doTry() throws ClientException, IOException {
                    List<TableSummary> tbls = client.listTables(databaseName);
                    boolean exist = false;
                    for (TableSummary tbl : tbls) {
                        if (tbl.getName().equals(tableName)) {
                            exist = true;
                            break;
                        }
                    }

                    if (!exist) {
                        throw new IOException(String.format(
                                "Not found table %s", tableName));
                    }
                }
            }, sessionName, conf.getRetryCount(), conf.getWaitSec());
        } catch (IOException e) {
            String msg = String.format(
                    "Cannot access table '%s', %s. " +
                    "Please check it with 'td table:list %s'. " +
                    "If it doesn't exist, please create it with 'td table:create %s %s'.",
                    tableName, e.getMessage(), databaseName, databaseName, tableName);
            System.out.println(msg);
            LOG.severe(msg);
            err.error = e;
        }
        return err;
    }

    public static TaskResult createSession(final BulkImportClient client, final UploadConfiguration conf,
            final String sessionName, final String databaseName, final String tableName) throws UploadPartsException {
        String msg = String.format("Create %s bulk_import session", sessionName);
        System.out.println(msg);
        LOG.info(msg);

        TaskResult err = new TaskResult();
        try {
            retryClient.retry(new Retryable2() {
                @Override
                public void doTry() throws ClientException, IOException {
                    client.createSession(sessionName, databaseName, tableName);
                }
            }, sessionName, conf.getRetryCount(), conf.getWaitSec());
        } catch (IOException e) {
            String emsg = String.format(
                    "Cannot create bulk_import session %s by using %s:%s, %s. ",
                    sessionName, databaseName, tableName, e.getMessage());
            System.out.println(emsg);
            LOG.severe(emsg);
            err.error = e;
        }
        return err;
    }

    public static TaskResult checkSession(final BulkImportClient client, final UploadConfiguration conf,
            final String sessionName) throws UploadPartsException {
        LOG.info(String.format("Check bulk_import session %s", sessionName));

        TaskResult err = new TaskResult();
        try {
            retryClient.retry(new Retryable2() {
                @Override
                public void doTry() throws ClientException, IOException {
                    client.showSession(sessionName);
                }
            }, sessionName, conf.getRetryCount(), conf.getWaitSec());
        } catch (IOException e) {
            String msg = String.format(
                    "Cannot access bulk_import session %s, %s. " +
                    "Please check it with 'td bulk_import:list'. " +
                    "If it doesn't exist, please create it.",
                    sessionName, e.getMessage());
            System.out.println(msg);
            LOG.severe(msg);
            err.error = e;
        }
        return err;
    }

    public static TaskResult deleteSession(final BulkImportClient client, final UploadConfiguration conf,
            final String sessionName) throws UploadPartsException {
        String msg = String.format("Delete bulk_import session %s", sessionName);
        System.out.println(msg);
        LOG.info(msg);

        TaskResult err = new TaskResult();
        try {
            retryClient.retry(new Retryable2() {
                @Override
                public void doTry() throws ClientException, IOException {
                    client.deleteSession(sessionName);
                }
            }, sessionName, conf.getRetryCount(), conf.getWaitSec());
        } catch (IOException e) {
            String emsg = String.format(
                    "Cannot delete bulk_import session %s, %s. " +
                    "Please check it with 'td bulk_import:list'.",
                    sessionName, e.getMessage());
            System.out.println(emsg);
            LOG.severe(emsg);
            err.error = e;
        }
        return err;
    }

}
