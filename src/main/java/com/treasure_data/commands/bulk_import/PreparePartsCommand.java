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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.treasure_data.commands.Command;
import com.treasure_data.commands.CommandException;
import com.treasure_data.utils.FileParser;
import com.treasure_data.utils.FileParserFactory;
import com.treasure_data.utils.MsgpackGZIPFileWriter;

public class PreparePartsCommand extends
        Command<PreparePartsRequest, PreparePartsResult> {
    private static final Logger LOG = Logger
            .getLogger(PreparePartsCommand.class.getName());

    private static BlockingQueue<Worker.Task> taskQueue;
    private List<Worker> workers;

    public PreparePartsCommand() {
    }

    @Override
    public void execute(PreparePartsRequest request, PreparePartsResult result)
            throws CommandException {
        LOG.info("Execute " + request.getName() + " command");

        Properties props = request.getProperties();
        File[] files = request.getFiles();

        // create queue and put all tasks to it
        taskQueue = new LinkedBlockingQueue<Worker.Task>(files.length);
        for (int i = 0; i < files.length; i++) {
            try {
                taskQueue.put(new Worker.Task(files[i]));
            } catch (InterruptedException e) {
                throw new CommandException(e);
            }
        }

        // avaiable cpu processors
        int numOfProcs = Runtime.getRuntime().availableProcessors();
        LOG.info("Recognized CPU Processors: " + numOfProcs);

        // create worker threads
        workers = new ArrayList<Worker>(numOfProcs);
        for (int i = 0; i < numOfProcs; i++) {
            Worker w = new Worker(props, request, result);
            LOG.fine("Created worker thread: " + w.getName());
            workers.add(w);
        }
        // start workers
        for (Worker w : workers) {
            w.start();
        }

        // join
        while (!workers.isEmpty()) {
            Worker lastWorker = workers.get(workers.size() - 1);
            try {
                lastWorker.join();
            } catch (InterruptedException e) {
                lastWorker.interrupt();
                try {
                    lastWorker.join();
                } catch (InterruptedException e2) {
                }
            }
            workers.remove(workers.size() - 1);
        }

        LOG.info("Finish " + request.getName() + " command");
    }

    static class Worker extends Thread {
        static class Task {
            File file;

            Task(File file) {
                this.file = file;
            }
        }

        Properties props;
        PreparePartsRequest request;
        PreparePartsResult result;

        public Worker(Properties props, PreparePartsRequest request,
                PreparePartsResult result) {
            this.props = props;
            this.request = request;
            this.result = result;
        }

        @Override
        public void run() {
            while (true) {
                Task t = taskQueue.poll();
                if (t == null) {
                    break;
                } else {
                    try {
                        execute(props, request, result, t.file);
                    } catch (CommandException e) {
                        LOG.severe("Failed command by " + getName() + ": "
                                + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }

        private void execute(Properties props, PreparePartsRequest request,
                PreparePartsResult result, final File infile)
                throws CommandException {
            LOG.info("Read file: " + infile.getName() + " by " + getName());

            FileParser p = null;
            MsgpackGZIPFileWriter w = null;
            String compressType = getCompressType(request, infile);
            try {
                p = FileParserFactory.newInstance(request);
                p.doPreExecute(createFileInputStream(compressType, infile));

                if (request.dryRun()) {
                    // if this processing is dry-run mode, thread of control
                    // returns back
                    return;
                }

                p.initReader(createFileInputStream(compressType, infile));
                p.setErrorRecordWriter(createErrorRecordOutputStream(request,
                        infile.getName()));
                w = new MsgpackGZIPFileWriter(request, infile.getName());
                while (p.parseRow(w)) {
                    ;
                }
            } finally {
                if (p != null) {
                    p.closeSilently();
                }
                if (w != null) {
                    w.closeSilently();
                }
                // TODO
            }

            result.setParsedRowNum(p.getRowNum());
            result.setWrittenRowNum(w.getRowNum());

            LOG.info("file: " + infile.getName() + ": "
                    + result.getParsedRowNum() + " entries by " + getName());
        }
    }

    private static OutputStream createErrorRecordOutputStream(
            PreparePartsRequest request, String infileName)
            throws CommandException {
        // outputDir
        String outputDirName = request.getErrorRecordOutputDirName();
        if (outputDirName == null) {
            return null;
        }

        // outputFilePrefix
        int lastSepIndex = infileName.lastIndexOf(File.pathSeparator);
        String outputFilePrefix = infileName.substring(lastSepIndex + 1,
                infileName.length()).replace('.', '_');
        String outputFileName = outputFilePrefix + ".err.txt";

        try {
            File outputFile = new File(outputDirName, outputFileName);
            LOG.info("Created output file: " + outputFileName);
            return new BufferedOutputStream(new FileOutputStream(outputFile));
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    private static InputStream createFileInputStream(String compressType,
            final File infile) throws CommandException {
        try {
            if (compressType.equals("gzip")) {
                return new GZIPInputStream(new FileInputStream(infile));
            } else if (compressType.equals("none")) {
                return new FileInputStream(infile);
            } else {
                throw new CommandException("unsupported compress type: "
                        + compressType);
            }
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    private static String getCompressType(PreparePartsRequest request,
            final File infile) throws CommandException {
        String fileName = infile.getName();
        String userCompressType = request.getCompressType();
        if (userCompressType == null) {
            throw new CommandException("fatal error");
        }

        String[] candidateCompressTypes;
        if (userCompressType.equals("gzip")) {
            candidateCompressTypes = new String[] { "gzip" };
        } else if (userCompressType.equals("none")) {
            candidateCompressTypes = new String[] { "none" };
        } else if (userCompressType.equals("auto")) {
            candidateCompressTypes = new String[] { "gzip", "none" };
        } else {
            throw new CommandException("unsupported compression type: "
                    + userCompressType);
        }

        String compressType = null;
        for (int i = 0; i < candidateCompressTypes.length; i++) {
            InputStream in = null;
            try {
                if (candidateCompressTypes[i].equals("gzip")) {
                    in = new GZIPInputStream(new FileInputStream(fileName));
                } else if (candidateCompressTypes[i].equals("none")) {
                    in = new FileInputStream(fileName);
                } else {
                    throw new CommandException("fatal error");
                }
                byte[] b = new byte[2];
                in.read(b);

                compressType = candidateCompressTypes[i];
                break;
            } catch (IOException e) {
                LOG.info(String.format("file %s is %s", fileName,
                        e.getMessage()));
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }

        if (compressType == null) {
            throw new CommandException(new IOException(String.format(
                    "cannot read file %s with specified compress type: %s",
                    fileName, userCompressType)));
        }

        return compressType;
    }
}
