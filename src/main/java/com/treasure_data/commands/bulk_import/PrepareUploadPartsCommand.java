package com.treasure_data.commands.bulk_import;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.RetryClient;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.client.RetryClient.Retryable;
import com.treasure_data.client.bulkimport.BulkImportClient;
import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.MultithreadsCommand;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.SessionSummary;

public class PrepareUploadPartsCommand extends
        UploadPartsCommand<PrepareUploadPartsRequest, PrepareUploadPartsResult> {
    private static final Logger LOG = Logger
            .getLogger(PrepareUploadPartsCommand.class.getName());

    static BlockingQueue<UploadWorker.Task> uploadTaskQueue =
            new LinkedBlockingQueue<UploadWorker.Task>();
    static List<UploadWorker> uploadWorkers = new ArrayList<UploadWorker>();

    public PrepareUploadPartsCommand() {
    }

    @Override
    public void execute(PrepareUploadPartsRequest request,
            PrepareUploadPartsResult result) throws CommandException {
        int numOfUploadThreads = request.getNumOfUploadThreads();

        LOG.fine(String.format("started uploading threads"));
        UploadPartsCommand uploadCommand = new UploadPartsCommand();
        UploadPartsRequest uploadRequest = request.getUploadPartsRequest();
        UploadPartsResult uploadResult = result.getUploadPartsResult();

        // create workers
        for (int i = 0; i < numOfUploadThreads; i++) {
            UploadWorker worker = new UploadWorker(this, uploadCommand,
                    uploadRequest, uploadResult);
            uploadWorkers.add(worker);
        }

        // start workers
        for (int i = 0; i < uploadWorkers.size(); i++) {
            uploadWorkers.get(i).start();
        }

        //LOG.fine(String.format("started preparing file: %s", file.getName()));
        PreparePartsRequest prepareRequest = request.getPreparePartsRequest();
        PrepareUploadPartsResult.MultiThreadsPreparePartsResult prepareResult =
                (PrepareUploadPartsResult.MultiThreadsPreparePartsResult)
                result.getPreparePartsResult();
        PreparePartsCommand prepareCommand = new PreparePartsCommand();
        MultithreadsCommand multithreads = new MultithreadsCommand(prepareCommand);
        multithreads.execute(prepareRequest, prepareResult);
        //prepareCommand.execute(prepareRequest, prepareResult, file);
        
        prepareResult.addFinishTask(numOfUploadThreads);

        postExecute(request, result);
    }

    @Override
    public void execute(PrepareUploadPartsRequest request,
            PrepareUploadPartsResult result, File file) throws CommandException {
        throw new UnsupportedOperationException();
    }

    static class UploadWorker extends Thread {
        static final Task FINISH_TASK = new Task("__FINISH__");
        AtomicBoolean isFinished = new AtomicBoolean(false);

        static class Task {
            String fileName;

            Task(String fileName) {
                this.fileName = fileName;
            }

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof Task)) {
                    return false;
                }

                Task t = (Task) obj;
                return t.fileName.equals(fileName);
            }

            static boolean endTask(Task t) {
                return t.equals(FINISH_TASK);
            }
        }

        PrepareUploadPartsCommand parent;
        UploadPartsCommand command;
        UploadPartsRequest request;
        UploadPartsResult result;

        UploadWorker(PrepareUploadPartsCommand parent, UploadPartsCommand command,
                UploadPartsRequest request, UploadPartsResult result) {
            this.parent = parent;
            this.command = command;
            this.request = request;
            this.result = result;
        }

        @Override
        public void run() {
            while (true) {
                UploadWorker.Task t = PrepareUploadPartsCommand.uploadTaskQueue.poll();
                if (t == null) {
                    continue;
                } else if (Task.endTask(t)) {
                    break;
                } else {
                    try {
                        run0(t);
                    } catch (CommandException e) {
                        LOG.severe(String.format("failed command by %s: %s",
                                getName(), e.getMessage()));
                        e.printStackTrace();
                    }
                }
            }
            isFinished.set(true);
        }

        private void run0(UploadWorker.Task t) throws CommandException {
            command.execute(request, result, new File(t.fileName));
        }
    }

    // TODO summary object works on multi-threading? => maybe ok by #MN
    private SessionSummary summary;

    @Override
    public void postExecute(PrepareUploadPartsRequest request,
            PrepareUploadPartsResult result) throws CommandException {
        // join
        while (!uploadWorkers.isEmpty()) {
            UploadWorker lastWorker = uploadWorkers.get(uploadWorkers.size() - 1);
            if (lastWorker.isFinished.get()) {
                uploadWorkers.remove(uploadWorkers.size() - 1);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        if (!request.autoPerform()) {
            return;
        }

        final TreasureDataClient client = new TreasureDataClient(
                request.getProperties());
        final BulkImportClient biClient = new BulkImportClient(client);

        final Session sess = new Session(request.getSessionName(), null, null);

        // freeze
        try {
            new RetryClient().retry(new Retryable() {
                @Override
                public void doTry() throws ClientException {
                    LOG.fine(String.format("freezing session %s",
                            sess.getName()));
                    biClient.freezeSession(sess);
                }
            }, request.getRetryCount(), request.getWaitSec());
        } catch (IOException e) {
            LOG.severe(e.getMessage());
            throw new CommandException(e);
        }

        // perform
        try {
            new RetryClient().retry(new Retryable() {
                @Override
                public void doTry() throws ClientException {
                    LOG.fine(String.format("performing all data on session %s",
                            sess.getName()));
                    biClient.performSession(sess);
                }
            }, request.getRetryCount(), request.getWaitSec());
        } catch (IOException e) {
            LOG.severe(e.getMessage());
            throw new CommandException(e);
        }
        try { // show job_id after starting perform processing
            new RetryClient().retry(new Retryable() {
                @Override
                public void doTry() throws ClientException {
                    summary = biClient.showSession(sess.getName());
                }
            }, request.getRetryCount(), request.getWaitSec());
        } catch (IOException e) {
            LOG.severe(e.getMessage());
            throw new CommandException(e);
        }
        LOG.info(String.format(
                "Job %s is queued", summary.getJobID()));
        LOG.info(String.format(
                "Use 'td job:show [-w] %s' to show the status", summary.getJobID()));

        if (request.autoCommit() && request.autoPerform()) {
            // check 'perform' processing is finished
            try {
                while (true) {
                    new RetryClient().retry(new Retryable() {
                        @Override
                        public void doTry() throws ClientException {
                            LOG.fine(String.format(
                                    "showing status of session %s",
                                    sess.getName()));
                            summary = biClient.showSession(sess.getName());
                        }
                    }, request.getRetryCount(), request.getWaitSec());
                    LOG.fine(String.format(
                            "current status of session %s is %s",
                            summary.getName(), summary.getStatus()));
                    if (summary.getStatus() == "ready") {
                        break;
                    } else if (summary.getStatus() == "uploading") {
                        throw new CommandException("Perform session failed");
                    }

                    // wait
                    try {
                        Thread.sleep(3 * 1000);
                    } catch (InterruptedException e) {
                    }
                }
            } catch (IOException e) {
                throw new CommandException(e);
            }

            // check error records
            try {
                new RetryClient().retry(new Retryable() {
                    @Override
                    public void doTry() throws ClientException {
                        LOG.fine(String.format(
                                "checking error records in session %s",
                                sess.getName()));
                        Unpacker unpacker = biClient.getErrorRecords(sess);
                        if (unpacker != null && unpacker.iterator().hasNext()) {
                            // if it has error records, it finished processing.
                            // it doesn't do commit processing.
                            throw new ClientException(new IOException(String.format(
                                    "detected error records on session %s",
                                    sess.getName())));
                        }
                    }
                }, request.getRetryCount(), request.getWaitSec());
            } catch (IOException e) {
                LOG.severe(e.getMessage());
                throw new CommandException(e);
            }

            // commit
            try {
                new RetryClient().retry(new Retryable() {
                    @Override
                    public void doTry() throws ClientException {
                        LOG.fine(String.format(
                                "commtting all data on session %s",
                                sess.getName()));
                        biClient.commitSession(sess);
                    }
                }, request.getRetryCount(), request.getWaitSec());
            } catch (IOException e) {
                LOG.severe(e.getMessage());
                throw new CommandException(e);
            }
        }
    }
}
