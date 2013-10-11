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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.td_import.TaskResult;
import com.treasure_data.td_import.prepare.MultiThreadPrepareProcessor;
import com.treasure_data.td_import.prepare.PrepareConfiguration;
import com.treasure_data.td_import.upload.MultiThreadUploadProcessor;
import com.treasure_data.td_import.upload.TableImportConfiguration;

public class TableImport extends Import {
    private static final Logger LOG = Logger.getLogger(TableImport.class.getName());

    public TableImport(Properties props) {
        super(props);
    }

    public List<com.treasure_data.td_import.TaskResult<?>> tableImport(final String[] args)
            throws Exception {
        // create configuration for 'table:import' processing
        TableImportConfiguration importConf = createTableImportConfiguration(args);

        // create TreasureDataClient object
        TreasureDataClient tdClient = new TreasureDataClient(importConf.getProperties());

        TaskResult<?> r = null;
        String databaseName = getDatabaseName(importConf); // database exists? TODO
        String tableName = getTableName(importConf); // table exists? TODO
        // get and extract uploaded files from command-line arguments
        String[] fileNames = getFileNames(importConf, 2);

        MultiThreadUploadProcessor uploadProc = createAndStartUploadProcessor(importConf);

        List<com.treasure_data.td_import.TaskResult<?>> results =
                new ArrayList<com.treasure_data.td_import.TaskResult<?>>();

        // create configuration for 'prepare' processing
        // TODO final PrepareConfiguration prepareConf = createPrepareConfiguration(args, true);

        MultiThreadPrepareProcessor prepareProc =
                createAndStartPrepareProcessor(importConf);

        // create sequential upload (prepare) tasks
        com.treasure_data.td_import.prepare.Task[] tasks = createSequentialImportTasks(
                databaseName, tableName, fileNames);

        // start sequential upload (prepare) tasks
        startPrepareTasks(importConf, tasks);

        results.addAll(stopPrepareProcessor(prepareProc));
        if (!hasNoPrepareError(results)) {
            return results;
        }

        // end of file list
        try {
            MultiThreadUploadProcessor.addFinishTask(importConf);
        } catch (Throwable t) {
            LOG.severe("Error occurred During 'addFinishTask' method call");
            LOG.throwing("Main", "addFinishTask", t);
        }

        results.addAll(stopUploadProcessor(uploadProc));
        if (!hasNoUploadError(results)) {
            return results;
        }

        return results;
    }

    protected String getDatabaseName(PrepareConfiguration conf) {
        return conf.getNonOptionArguments().get(0);
    }

    protected String getTableName(PrepareConfiguration conf) {
        return conf.getNonOptionArguments().get(1);
    }

    protected String[] getFileNames(PrepareConfiguration conf, int filePos) {
        List<String> argList = conf.getNonOptionArguments();
        final String[] fileNames = new String[argList.size() - filePos];
        for (int i = 0; i < fileNames.length; i++) {
            fileNames[i] = argList.get(i + filePos);
        }
        return fileNames;
    }

    protected boolean hasNoPrepareError(List<com.treasure_data.td_import.TaskResult<?>> results) {
        boolean hasNoError = true;
        for (com.treasure_data.td_import.TaskResult<?> result : results) {
            if (! (result instanceof com.treasure_data.td_import.prepare.TaskResult)) {
                continue;
            }

            if (result.error != null) {
                hasNoError = false;
                break;
            }
        }
        return hasNoError;
    }

    protected boolean hasNoUploadError(List<com.treasure_data.td_import.TaskResult<?>> results) {
        boolean hasNoError = true;
        for (com.treasure_data.td_import.TaskResult<?> result : results) {
            if (! (result instanceof com.treasure_data.td_import.upload.TaskResult)) {
                continue;
            }

            if (result.error != null) {
                hasNoError = false;
                break;
            }
        }
        return hasNoError;
    }

    protected MultiThreadPrepareProcessor createAndStartPrepareProcessor(
            PrepareConfiguration conf) {
        MultiThreadPrepareProcessor proc = new MultiThreadPrepareProcessor(conf);
        proc.registerWorkers();
        proc.startWorkers();
        return proc;
    }

    protected List<com.treasure_data.td_import.TaskResult<?>> stopPrepareProcessor(
            MultiThreadPrepareProcessor proc) {
        // wait for finishing prepare processing
        proc.joinWorkers();

        // wait for finishing prepare processing
        List<com.treasure_data.td_import.TaskResult<?>> results =
                new ArrayList<com.treasure_data.td_import.TaskResult<?>>();
        results.addAll(proc.getTaskResults());
        return results;
    }

    protected com.treasure_data.td_import.prepare.Task[] createSequentialImportTasks(
            final String databaseName, final String tableName, final String[] fileNames) {
        com.treasure_data.td_import.prepare.Task[] tasks =
                new com.treasure_data.td_import.prepare.Task[fileNames.length];
        for (int i = 0; i < fileNames.length; i++) {
            tasks[i] = new com.treasure_data.td_import.prepare.SequentialImportTask(
                    databaseName, tableName, fileNames[i]);
        }
        return tasks;
    }

    protected void startPrepareTasks(
            final PrepareConfiguration conf,
            final com.treasure_data.td_import.prepare.Task[] tasks) {
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < tasks.length; i++) {
                    try {
                        MultiThreadPrepareProcessor.addTask(tasks[i]);
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
    }

    protected MultiThreadUploadProcessor createAndStartUploadProcessor(TableImportConfiguration conf) {
        MultiThreadUploadProcessor proc = new MultiThreadUploadProcessor(conf);
        proc.registerWorkers();
        proc.startWorkers();
        return proc;
    }

    protected List<com.treasure_data.td_import.TaskResult<?>> stopUploadProcessor(
            MultiThreadUploadProcessor proc) {
        // wait for finishing upload processing
        proc.joinWorkers();

        // wait for finishing upload processing
        List<com.treasure_data.td_import.TaskResult<?>> results =
                new ArrayList<com.treasure_data.td_import.TaskResult<?>>();
        results.addAll(proc.getTaskResults());
        return results;
    }

    protected TableImportConfiguration createTableImportConfiguration(String[] args) {
        TableImportConfiguration.Factory fact = new TableImportConfiguration.Factory(props);
        TableImportConfiguration conf = fact.newUploadConfiguration(args);

        showHelp(Configuration.Command.TABLEIMPORT, conf, args);

        conf.configure(props, fact.getTableImportOptions());
        return conf;
    }

    protected void showHelp(Configuration.Command cmd, PrepareConfiguration conf, String[] args) {
        if (conf.hasHelpOption()) {
            System.out.println(cmd.showHelp(conf, props));
            System.exit(0);
        }
    }

}
