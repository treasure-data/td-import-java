package com.treasure_data.commands.bulk_import;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.RetryClient;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.client.RetryClient.Retryable;
import com.treasure_data.client.bulkimport.BulkImportClient;
import com.treasure_data.commands.CommandException;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.SessionSummary;

public class PrepareUploadPartsCommand extends
        UploadPartsCommand<PrepareUploadPartsRequest, PrepareUploadPartsResult> {
    private static final Logger LOG = Logger
            .getLogger(PrepareUploadPartsCommand.class.getName());

    BlockingQueue<Worker.Task> taskQueue;

    public PrepareUploadPartsCommand() {
        taskQueue = new LinkedBlockingQueue<Worker.Task>();
    }

    @Override
    public void execute(PrepareUploadPartsRequest request,
            PrepareUploadPartsResult result) throws CommandException {
        File[] files = request.getFiles();
        for (File f : files) {
            PrepareUploadPartsResult clonedResult =
                    (PrepareUploadPartsResult) result.clone();
            execute(request, clonedResult, f);
        }
    }

    @Override
    public void execute(PrepareUploadPartsRequest request,
            PrepareUploadPartsResult result, File file) throws CommandException {
        int numOfUploadThreads = request.getNumOfUploadThreads();

        LOG.fine(String.format("started preparing file: %s", file.getName()));
        PreparePartsRequest prepareRequest = request.getPreparePartsRequest();
        MultiThreadsPreparePartsResult prepareResult =
                (MultiThreadsPreparePartsResult) result.getPreparePartsResult();
        prepareResult.setPrepareUploadPartsCommand(this);
        PreparePartsCommand prepareCommand = new PreparePartsCommand();
        prepareCommand.execute(prepareRequest, prepareResult, file);
        prepareResult.addFinishTask(numOfUploadThreads);

        UploadPartsCommand uploadCommand = new UploadPartsCommand();
        UploadPartsRequest uploadRequest = request.getUploadPartsRequest();
        UploadPartsResult uploadResult = result.getUploadPartsResult();
        List<Worker> workers = new ArrayList<Worker>(numOfUploadThreads);

        //create workers
        for (int i = 0; i < numOfUploadThreads; i++) {
            Worker worker = new Worker(this, uploadCommand, uploadRequest, uploadResult);
            workers.add(worker);
        }

        // start workers
        for (int i = 0; i < workers.size(); i++) {
            workers.get(i).start();
        }

        // join
        while (!workers.isEmpty()) {
            Worker lastWorker = workers.get(workers.size() - 1);
            System.out.println("last worker done: " + lastWorker.isFinished.get());
            if (lastWorker.isFinished.get()) {
                workers.remove(workers.size() - 1);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO
            }
        }
        System.out.println("### 2");
    }

    static class Worker extends Thread {
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

        Worker(PrepareUploadPartsCommand parent, UploadPartsCommand command,
                UploadPartsRequest request, UploadPartsResult result) {
            this.parent = parent;
            this.command = command;
            this.request = request;
            this.result = result;
        }

        @Override
        public void run() {
            while (true) {
                Worker.Task t = parent.taskQueue.poll();
                System.out.println("task: " + t.fileName);
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

        private void run0(Worker.Task t) throws CommandException {
            command.execute(request, result, new File(t.fileName));
        }
    }

    // TODO summary object works on multi-threading?
    private SessionSummary summary;

    @Override
    public void postExecute(PrepareUploadPartsRequest request,
            PrepareUploadPartsResult result) throws CommandException {
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

            // check error records FIXME
//            try {
//                new RetryClient().retry(new Retryable() {
//                    @Override
//                    public void doTry() throws ClientException {
//                        LOG.fine(String.format(
//                                "checking error records in session %s",
//                                sess.getName()));
//                        // TODO
//                        Unpacker unpacker = biClient.getErrorRecords(sess);
//                        if (unpacker != null) {
//                            UnpackerIterator iter = unpacker.iterator();
//                            while (iter.hasNext()) {
//                                // TODO
//                                System.out.println(iter.next());
//                            }
//                        }
//                    }
//                }, request.getRetryCount(), request.getWaitSec());
//            } catch (IOException e) {
//                LOG.severe(e.getMessage());
//                throw new CommandException(e);
//            }

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
