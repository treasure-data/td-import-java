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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.logging.Logger;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.HttpClientException;
import com.treasure_data.client.RetryClient;
import com.treasure_data.client.RetryClient.Retryable;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.client.bulkimport.BulkImportClient;
import com.treasure_data.commands.Command;
import com.treasure_data.commands.CommandException;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.UploadPartRequest;

public class UploadPartsCommand<REQ extends UploadPartsRequest, RET extends UploadPartsResult>
        extends Command<REQ, RET> {

    private static final Logger LOG = Logger.getLogger(UploadPartsCommand.class
            .getName());

    public UploadPartsCommand() {
    }

    private static class ExtRetryClient extends RetryClient {
        public void retry(Retryable r, int retryCount, long waitSec) throws IOException {
            int count = 0;
            boolean notRetry = false;
            while (true) {
                try {
                    r.doTry();
                    break;
                } catch (ClientException e) {
                    LOG.warning(e.getMessage());
                    if (e instanceof HttpClientException
                            && ((HttpClientException) e).getResponseCode() < 400) {
                        count++;
                        waitRetry(waitSec);
                    } else if (e.getCause() instanceof FileNotFoundException) {
                        count++;
                        waitRetry(waitSec);
                    } else {
                        LOG.info("turned notRetry flag: " + notRetry);
                        notRetry = true;
                    }
                } finally {
                    if (count >= retryCount || notRetry) {
                        throw new IOException("Retry out error");
                    }
                }
            }
        }
    }

    @Override
    public void execute(final REQ request, final RET result, final File file)
            throws CommandException {
        LOG.fine(String.format("started uploading file: %s", file.getName()));

        final TreasureDataClient client = new TreasureDataClient(
                request.getProperties());
        final BulkImportClient biClient = new BulkImportClient(client);

        final Session sess = new Session(request.getSessionName(), null, null);
        final String partID = file.getName().replace('.', '_');

        // upload records
        try {
            new ExtRetryClient().retry(new Retryable() {
                @Override
                public void doTry() throws ClientException {
                    try {
                        InputStream in = new BufferedInputStream(
                                new FileInputStream(file));
                        long size = file.length();

                        LOG.fine(String.format(
                                "uploading file = %s on session %s",
                                file.getName(), sess.getName()));
                        UploadPartRequest req = new UploadPartRequest(sess,
                                partID, in, (int) size);
                        // TODO #MN change client's API: int to long
                        biClient.uploadPart(req);
                    } catch (IOException e) {
                        throw new ClientException(e);
                    }
                }
            }, request.getRetryCount(), request.getWaitSec());
        } catch (IOException e) {
            LOG.severe(e.getMessage());
            throw new CommandException(e);
        }

        LOG.fine(String.format("uploaded file: %s", file.getName()));
    }

    @Override
    public void preExecute(REQ request, RET result) throws CommandException {
    }

    @Override
    public void postExecute(REQ request, RET result) throws CommandException {
    }
}
