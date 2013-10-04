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
package com.treasure_data.td_import;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.client.bulkimport.BulkImportClient;
import com.treasure_data.model.bulkimport.SessionSummary;
import com.treasure_data.td_import.TaskResult;
import com.treasure_data.td_import.prepare.MultiThreadPrepareProcessor;
import com.treasure_data.td_import.prepare.PrepareConfiguration;
import com.treasure_data.td_import.prepare.SequentialUploadTask;
import com.treasure_data.td_import.upload.MultiThreadUploadProcessor;
import com.treasure_data.td_import.upload.UploadConfiguration;
import com.treasure_data.td_import.upload.UploadProcessor;

public class BulkImportMain {
    private static final Logger LOG = Logger.getLogger(BulkImportMain.class.getName());

    public static void prepare(final String[] args, Properties props)
            throws Exception {
        LOG.info(String.format("Start '%s' command", Configuration.CMD_PREPARE));

        // create configuration for 'prepare' processing
        final PrepareConfiguration conf = createPrepareConfiguration(props, args);

        // extract command-line arguments
        List<String> argList = conf.getNonOptionArguments();
        final String[] fileNames = new String[argList.size() - 1]; // delete 'prepare_parts'
        for (int i = 0; i < fileNames.length; i++) {
            fileNames[i] = argList.get(i + 1);
        }

        showPrepare(fileNames, conf.getOutputDirName());

        MultiThreadPrepareProcessor proc = new MultiThreadPrepareProcessor(conf);
        proc.registerWorkers();
        proc.startWorkers();

        // scan files that are uploaded
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < fileNames.length; i++) {
                    try {
                        com.treasure_data.td_import.prepare.Task task =
                                new com.treasure_data.td_import.prepare.Task(
                                        fileNames[i]);
                        MultiThreadPrepareProcessor.addTask(task);
                    } catch (Throwable t) {
                        LOG.severe("Error occurred During 'addTask' method call");
                        LOG.throwing("Main", "addTask", t);
                    }
                }

                // end of file list
                try {
                    MultiThreadPrepareProcessor.addFinishTask(conf);
                } catch (Throwable t) {
                    LOG.severe("Error occurred During 'addFinishTask' method call");
                    LOG.throwing("Main", "addFinishTask", t);
                }
            }
        }).start();

        proc.joinWorkers();
        List<com.treasure_data.td_import.prepare.TaskResult> prepareResults = proc
                .getTaskResults();

        showPrepareResults(prepareResults);
        listNextStepOfPrepareProc(prepareResults);

        LOG.info(String.format("Finished '%s' command", Configuration.CMD_PREPARE));
    }

    public static void upload(final String[] args, Properties props)
            throws Exception {
        LOG.info(String.format("Start '%s' command", Configuration.CMD_UPLOAD));

        // create configuration for 'upload' processing
        final UploadConfiguration uploadConf = createUploadConfiguration(props, args);

        // extract command-line arguments
        List<String> argList = uploadConf.getNonOptionArguments();

        // create TreasureDataClient and BulkImportClient objects
        TreasureDataClient tdClient = new TreasureDataClient(uploadConf.getProperties());
        BulkImportClient biClient = new BulkImportClient(tdClient);

        // configure session name
        TaskResult e = null;
        final String sessionName;
        int filePos;
        if (uploadConf.autoCreate()) { // 'auto-create-session'
            String databaseName = uploadConf.enableMake()[0];
            String tableName = uploadConf.enableMake()[1];
            Date d = new Date();
            String format = "yyyy_MM_dd";
            String timestamp = new SimpleDateFormat(format).format(d);
            sessionName = String.format("%s_%s_%s_%d", databaseName, tableName,
                    timestamp, (d.getTime() / 1000));

            // validate that database is live or not
            e = UploadProcessor.checkDatabase(tdClient, uploadConf,
                    sessionName, databaseName);
            if (e.error != null) {
                throw new IllegalArgumentException(e.error);
            }

            // validate that table is live or not
            e = UploadProcessor.checkTable(tdClient, uploadConf,
                    sessionName, databaseName, tableName);
            if (e.error != null) {
                // TODO FIXME #MN should create table automatically if
                // it is not found.
                throw new IllegalArgumentException(e.error);
            }

            // create bulk import session
            e = UploadProcessor.createSession(biClient, uploadConf, sessionName, databaseName, tableName);
            if (e.error != null) {
                throw new IllegalArgumentException(e.error);
            }

            filePos = 1;
        } else {
            sessionName = argList.get(1); // get session name from command-line arguments
            // validate that the session is live or not
            e = UploadProcessor.checkSession(biClient, uploadConf, sessionName);
            if (e.error != null) {
                throw new IllegalArgumentException(e.error);
            }

            filePos = 2;
        }

        // if session is already freezed, exception is thrown.
        SessionSummary sess = UploadProcessor.showSession(biClient, uploadConf, sessionName);
        if (sess.uploadFrozen()) {
            throw new IllegalArgumentException(String.format(
                    "Bulk import session %s is already freezed. Please check it with 'td import:show %s'",
                    sessionName, sessionName));
        }

        // configure uploaded file list
        final String[] fileNames = new String[argList.size() - filePos]; // delete command
        for (int i = 0; i < fileNames.length; i++) {
            fileNames[i] = argList.get(i + filePos);
        }

        showUpload(fileNames, sessionName);

        MultiThreadUploadProcessor uploadProc = new MultiThreadUploadProcessor(uploadConf);
        uploadProc.registerWorkers();
        uploadProc.startWorkers();

        List<com.treasure_data.td_import.prepare.TaskResult> prepareResults = null;

        if (!uploadConf.hasPrepareOptions()) {
            // scan files that are uploaded
            new Thread(new Runnable() {
                public void run() {
                    for (int i = 0; i < fileNames.length; i++) {
                        try {
                            long size = new File(fileNames[i]).length();
                            com.treasure_data.td_import.upload.UploadTask task =
                                    new com.treasure_data.td_import.upload.UploadTask(
                                    sessionName, fileNames[i], size);
                            MultiThreadUploadProcessor.addTask(task);
                        } catch (Throwable t) {
                            LOG.severe("Error occurred During 'addTask' method call");
                            LOG.throwing("Main", "addTask", t);
                        }
                    }

                    // end of file list
                    try {
                        MultiThreadUploadProcessor.addFinishTask(uploadConf);
                    } catch (Throwable t) {
                        LOG.severe("Error occurred During 'addFinishTask' method call");
                        LOG.throwing("Main", "addFinishTask", t);
                    }
                }
            }).start();
        } else {
            // create configuration for 'prepare' processing
            final PrepareConfiguration prepareConf = createPrepareConfiguration(props, args, true);

            MultiThreadPrepareProcessor prepareProc = new MultiThreadPrepareProcessor(prepareConf);
            prepareProc.registerWorkers();
            prepareProc.startWorkers();

            // scan files that are uploaded
            new Thread(new Runnable() {
                public void run() {
                    for (int i = 0; i < fileNames.length; i++) {
                        try {
                            SequentialUploadTask task = new SequentialUploadTask(sessionName, fileNames[i]);
                            MultiThreadPrepareProcessor.addTask(task);
                        } catch (Throwable t) {
                            LOG.severe("Error occurred During 'addTask' method call");
                            LOG.throwing("Main", "addTask", t);
                        }
                    }

                    // end of file list
                    try {
                        MultiThreadPrepareProcessor.addFinishTask(prepareConf);
                    } catch (Throwable t) {
                        LOG.severe("Error occurred During 'addFinishTask' method call");
                        LOG.throwing("Main", "addFinishTask", t);
                    }
                }
            }).start();

            prepareProc.joinWorkers();
            prepareResults = prepareProc.getTaskResults();
            showPrepareResults(prepareResults);
            listNextStepOfPrepareProc(prepareResults);

            // end of file list
            try {
                MultiThreadUploadProcessor.addFinishTask(uploadConf);
            } catch (Throwable t) {
                LOG.severe("Error occurred During 'addFinishTask' method call");
                LOG.throwing("Main", "addFinishTask", t);
            }
        }

        uploadProc.joinWorkers();
        List<com.treasure_data.td_import.upload.TaskResult> uploadResults = uploadProc
                .getTaskResults();
        showUploadResults(uploadResults);
        listNextStepOfUploadProc(uploadResults, sessionName);

        if (!hasNoUploadError(uploadResults)
                || (prepareResults != null && !hasNoPrepareError(prepareResults))) {
            return;
        }

        // 'auto-perform' and 'auto-commit'
        UploadProcessor.processAfterUploading(biClient, uploadConf, sessionName);

        if (uploadConf.autoDelete()) {
            // 'auto-delete-session'
            UploadProcessor.deleteSession(biClient, uploadConf, sessionName);
        }

        LOG.info(String.format("Finished '%s' command", Configuration.CMD_UPLOAD));
    }

    private static boolean hasNoPrepareError(List<com.treasure_data.td_import.prepare.TaskResult> results) {
        boolean hasNoError = true;
        for (com.treasure_data.td_import.prepare.TaskResult result : results) {
            if (result.error != null) {
                hasNoError = false;
                break;
            }
        }
        return hasNoError;
    }

    private static boolean hasNoUploadError(List<com.treasure_data.td_import.upload.TaskResult> results) {
        boolean hasNoError = true;
        for (com.treasure_data.td_import.upload.TaskResult result : results) {
            if (result.error != null) {
                hasNoError = false;
                break;
            }
        }
        return hasNoError;
    }

    private static void showPrepare(String[] fileNames, String outputDirName) {
        System.out.println();
        System.out.println("Preparing files");
        System.out.println(String.format("  Output dir   : %s", outputDirName));
        showFiles(fileNames);
        System.out.println();
    }

    private static void showUpload(String[] fileNames, String sessionName) {
        System.out.println();
        System.out.println("Uploading prepared files");
        System.out.println(String.format("  Session name : %s", sessionName));
        showFiles(fileNames);
        System.out.println();
    }

    private static void showFiles(String[] fileNames) {
        for (String fileName : fileNames) {
            System.out.println(String.format("  File       : %s (%d bytes)", fileName, new File(fileName).length()));
        }
    }

    private static PrepareConfiguration createPrepareConfiguration(Properties props, String[] args) {
        return createPrepareConfiguration(props, args, false);
    }

    private static PrepareConfiguration createPrepareConfiguration(Properties props, String[] args, boolean isUploaded) {
        PrepareConfiguration.Factory fact = new PrepareConfiguration.Factory(props, isUploaded);
        PrepareConfiguration conf = fact.newPrepareConfiguration(args);

        if (!isUploaded) {
            showHelp(Configuration.Command.PREPARE, conf, props, args);
        }

        conf.configure(props, fact.getBulkImportOptions());
        return conf;
    }

    private static UploadConfiguration createUploadConfiguration(Properties props, String[] args) {
        UploadConfiguration.Factory fact = new UploadConfiguration.Factory(props);
        UploadConfiguration conf = fact.newUploadConfiguration(args);

        showHelp(Configuration.Command.UPLOAD, conf, props, args);

        conf.configure(props, fact.getBulkImportOptions());
        return conf;
    }

    private static void showHelp(Configuration.Command cmd, PrepareConfiguration conf, Properties props, String[] args) {
        if (conf.hasHelpOption()) {
            System.out.println(cmd.showHelp(conf, props));
            System.exit(0);
        }
    }

    private static void showPrepareResults(List<com.treasure_data.td_import.prepare.TaskResult> results) {
        System.out.println();
        System.out.println("Prepare status:");
        for (com.treasure_data.td_import.prepare.TaskResult result : results) {
            String status;
            if (result.error == null) {
                status = Configuration.STAT_SUCCESS;
            } else {
                status = Configuration.STAT_ERROR;
            }
            System.out.println(String.format("  File    : %s", result.task.fileName));
            System.out.println(String.format("    Status          : %s", status));
            System.out.println(String.format("    Read lines      : %d", result.readLines));
            System.out.println(String.format("    Valid rows      : %d", result.convertedRows));
            System.out.println(String.format("    Invalid rows    : %d", result.invalidRows));
            int len = result.outFileNames.size();
            boolean first = true;
            for (int i = 0; i < len; i++) {
                if (first) {
                    System.out.println(String.format("    Converted Files : %s (%d bytes)",
                            result.outFileNames.get(i), result.outFileSizes.get(i)));
                    first = false;
                } else {
                    System.out.println(String.format("                      %s (%d bytes)",
                            result.outFileNames.get(i), result.outFileSizes.get(i)));
                }
            }
        }
        System.out.println();
    }

    private static void listNextStepOfPrepareProc(List<com.treasure_data.td_import.prepare.TaskResult> results) {
        System.out.println();
        System.out.println("Next steps:");

        List<String> readyToUploadFiles = new ArrayList<String>();

        for (com.treasure_data.td_import.prepare.TaskResult result : results) {
            if (result.error == null) {
                int len = result.outFileNames.size();
                // success
                for (int i = 0; i < len; i++) {
                    readyToUploadFiles.add(result.outFileNames.get(i));
                }
            } else {
                // error
                System.out.println(String.format(
                        "  => check td-bulk-import.log and original %s: %s.",
                        result.task.fileName, result.error.getMessage()));
            }
        }

        if(!readyToUploadFiles.isEmpty()) {
            System.out.println(String.format(
                        "  => execute following 'td import:upload' command. "
                        + "if the bulk import session is not created yet, please create it "
                        + "with 'td import:create <session> <database> <table>' command."));
            StringBuilder sb = new StringBuilder();
            sb.append("     $ td import:upload <session>");
            for(String file : readyToUploadFiles) {
                sb.append(" '");
                sb.append(file);
                sb.append("'");
            }
            System.out.println(sb);
        }
        System.out.println();
    }

    private static void showUploadResults(List<com.treasure_data.td_import.upload.TaskResult> results) {
        System.out.println();
        System.out.println("Upload status:");
        for (com.treasure_data.td_import.upload.TaskResult result : results) {
            String status;
            if (result.error == null) {
                status = Configuration.STAT_SUCCESS;
            } else {
                status = Configuration.STAT_ERROR;
            }
            com.treasure_data.td_import.upload.UploadTask task = (com.treasure_data.td_import.upload.UploadTask) result.task;
            System.out.println(String.format("  File    : %s", result.task.fileName));
            System.out.println(String.format("    Status          : %s", status));
            System.out.println(String.format("    Part name       : %s", task.partName));
            System.out.println(String.format("    Size            : %d", task.size));
            System.out.println(String.format("    Retry count     : %d", result.retryCount));
        }
        System.out.println();
    }

    private static void listNextStepOfUploadProc(List<com.treasure_data.td_import.upload.TaskResult> results,
            String sessionName) {
        System.out.println();
        System.out.println("Next Steps:");
        boolean hasErrors = false;
        for (com.treasure_data.td_import.upload.TaskResult result : results) {
            if (result.error != null) {
                // error
                System.out.println(String.format(
                        "  => check td-bulk-import.log and re-upload %s: %s.",
                        result.task.fileName, result.error.getMessage()));
                hasErrors = true;
            }
        }

        if (!hasErrors) {
            // success
            System.out.println(String.format(
                    "  => execute 'td import:perform %s'.",
                    sessionName));
        }

        System.out.println();
    }

    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Command not specified");
        }

        Properties props = System.getProperties();
        String commandName = args[0].toLowerCase();
        Configuration.Command cmd = Configuration.Command.fromString(commandName);
        if (cmd == null) {
            throw new IllegalArgumentException(
                    String.format("Not support command %s", commandName));
        }

        if (cmd.equals(Configuration.Command.PREPARE)) {
            prepare(args, props);
        } else if (cmd.equals(Configuration.Command.UPLOAD)) {
            upload(args, props);
        } else if (cmd.equals(Configuration.Command.AUTO)) {
            String[] args0 = new String[args.length + 3];
            args0[args.length] = "--auto-commit";
            args0[args.length + 1] = "--auto-perform";
            args0[args.length + 2] = "--auto-delete";
            System.arraycopy(args, 0, args0, 0, args.length);

            props.setProperty(Configuration.CMD_AUTO_ENABLE, "true");

            upload(args, props);
        } else {
            throw new UnsupportedOperationException("Fatal error");
        }
    }

}
