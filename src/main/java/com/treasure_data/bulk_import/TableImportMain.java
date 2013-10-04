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
package com.treasure_data.bulk_import;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import javax.swing.event.ListSelectionEvent;

import com.treasure_data.bulk_import.TaskResult;
import com.treasure_data.bulk_import.prepare.SequentialImportTask;
import com.treasure_data.bulk_import.prepare.MultiThreadPrepareProcessor;
import com.treasure_data.bulk_import.prepare.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare.SequentialUploadTask;
import com.treasure_data.bulk_import.upload.MultiThreadUploadProcessor;
import com.treasure_data.bulk_import.upload.TableImportConfiguration;
import com.treasure_data.bulk_import.upload.ImportTask;
import com.treasure_data.bulk_import.upload.UploadConfiguration;
import com.treasure_data.bulk_import.upload.UploadProcessor;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.client.bulkimport.BulkImportClient;
import com.treasure_data.model.bulkimport.SessionSummary;

public class TableImportMain {
    private static final Logger LOG = Logger.getLogger(TableImportMain.class.getName());

    public static void tableImport(final String[] args, Properties props)
            throws Exception {
        LOG.info(String.format("Start '%s' command", Configuration.CMD_TABLEIMPORT));
        // TODO

        // create configuration for 'table:import' processing TODO
        final TableImportConfiguration importConf = createTableImportConfiguration(props, args);

        // extract command-line arguments
        List<String> argList = importConf.getNonOptionArguments();

        // create TreasureDataClient and BulkImportClient objects
        TreasureDataClient tdClient = new TreasureDataClient(importConf.getProperties());

        // configure session name
        TaskResult e;

        // database exists? TODO
        final String databaseName = argList.get(0);
        // table exists? TODO
        final String tableName = argList.get(1);

        // configure imported file list
        final String[] fileNames = new String[argList.size() - 2]; // delete command
        for (int i = 0; i < fileNames.length; i++) {
            fileNames[i] = argList.get(i + 2);
        }

        // TODO showImport(fileNames, sessionName);

        MultiThreadUploadProcessor uploadProc = new MultiThreadUploadProcessor(importConf);
        uploadProc.registerWorkers();
        uploadProc.startWorkers();

        List<TaskResult> errs = new ArrayList<TaskResult>();
        List<com.treasure_data.bulk_import.prepare.TaskResult> prepareResults = null;

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
                        SequentialImportTask task = new SequentialImportTask(databaseName, tableName, fileNames[i]);
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
        // TODO showPrepareResults(prepareResults);
        // TODO listNextStepOfPrepareProc(prepareResults);

        // end of file list
        try {
            MultiThreadUploadProcessor.addFinishTask(importConf);
        } catch (Throwable t) {
            LOG.severe("Error occurred During 'addFinishTask' method call");
            LOG.throwing("Main", "addFinishTask", t);
        }

        uploadProc.joinWorkers();
        List<com.treasure_data.bulk_import.upload.TaskResult> uploadResults = uploadProc
                .getTaskResults();
        // TODO showUploadResults(uploadResults);
        // TODO listNextStepOfUploadProc(uploadResults, sessionName);

        if (!hasNoUploadError(uploadResults)
                || (prepareResults != null && !hasNoPrepareError(prepareResults))) {
            return;
        }

        LOG.info(String.format("Finished '%s' command", Configuration.CMD_TABLEIMPORT));
    }

    private static boolean hasNoPrepareError(List<com.treasure_data.bulk_import.prepare.TaskResult> results) {
        boolean hasNoError = true;
        for (com.treasure_data.bulk_import.prepare.TaskResult result : results) {
            if (result.error != null) {
                hasNoError = false;
                break;
            }
        }
        return hasNoError;
    }

    private static boolean hasNoUploadError(List<com.treasure_data.bulk_import.upload.TaskResult> results) {
        boolean hasNoError = true;
        for (com.treasure_data.bulk_import.upload.TaskResult result : results) {
            if (result.error != null) {
                hasNoError = false;
                break;
            }
        }
        return hasNoError;
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

    private static TableImportConfiguration createTableImportConfiguration(Properties props, String[] args) {
        TableImportConfiguration.Factory fact = new TableImportConfiguration.Factory(props);
        TableImportConfiguration conf = fact.newUploadConfiguration(args);

        showHelp(Configuration.Command.TABLEIMPORT, conf, props, args);

        conf.configure(props, fact.getTableImportOptions());
        return conf;
    }

    private static void showHelp(Configuration.Command cmd, PrepareConfiguration conf, Properties props, String[] args) {
        if (conf.hasHelpOption()) {
            System.out.println(cmd.showHelp(conf, props));
            System.exit(0);
        }
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

        if (cmd.equals(Configuration.Command.TABLEIMPORT)) {
            tableImport(args, props);
        } else {
            throw new UnsupportedOperationException("Fatal error");
        }
    }

}
