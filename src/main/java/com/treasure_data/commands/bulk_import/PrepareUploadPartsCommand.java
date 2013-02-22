package com.treasure_data.commands.bulk_import;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;

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

    public PrepareUploadPartsCommand() {
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
        LOG.fine(String.format("started preparing file: %s", file.getName()));
        PreparePartsRequest prepareRequest = request.getPreparePartsRequest();
        PreparePartsResult prepareResult = result.getPreparePartsResult();
        PreparePartsCommand prepareCommand = new PreparePartsCommand();
        prepareCommand.execute(prepareRequest, prepareResult, file);
        List<String> filePaths = prepareResult.getOutputFiles();

        // TODO
        // TODO #MN can optimize the method with multi-threading
        // TODO

        LOG.fine(String.format("started uploading file: %s", file.getName()));
        UploadPartsRequest uploadRequest = request.getUploadPartsRequest();
        uploadRequest.setFiles(filePaths.toArray(new String[0]));
        UploadPartsResult uploadResult = result.getUploadPartsResult();
        UploadPartsCommand uploadCommand = new UploadPartsCommand();
        MultithreadsCommand<UploadPartsRequest, UploadPartsResult> multithreading =
                new MultithreadsCommand<UploadPartsRequest, UploadPartsResult>(uploadCommand);
        multithreading.execute(uploadRequest, uploadResult);
    }

    private SessionSummary summary;

    @Override
    public void postExecute(PrepareUploadPartsRequest request,
            PrepareUploadPartsResult result) throws CommandException {
        final TreasureDataClient client = new TreasureDataClient(
                request.getProperties());
        final BulkImportClient biClient = new BulkImportClient(client);

        final Session sess = new Session(request.getSessionName(), null, null);

        if (request.autoPerform()) {
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
                        LOG.fine(String.format(
                                "performing all data on session %s",
                                sess.getName()));
                        biClient.performSession(sess);
                    }
                }, request.getRetryCount(), request.getWaitSec());
            } catch (IOException e) {
                LOG.severe(e.getMessage());
                throw new CommandException(e);
            }
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
