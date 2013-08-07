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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import joptsimple.OptionSet;

import com.treasure_data.bulk_import.ErrorInfo;
import com.treasure_data.bulk_import.prepare_parts.MultiThreadPrepareProcessor;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.UploadTask;
import com.treasure_data.bulk_import.upload_parts.MultiThreadUploadProcessor;
import com.treasure_data.bulk_import.upload_parts.UploadConfiguration;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.client.bulkimport.BulkImportClient;

public class BulkImportMain {
    private static final Logger LOG = Logger.getLogger(BulkImportMain.class.getName());

    public static void prepareParts(final String[] args, Properties props)
            throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("File names not specified");
        }

        String msg = String.format("Start %s command", Configuration.CMD_PREPARE_PARTS);
        System.out.println(msg);
        LOG.info(msg);

        final String[] fileNames = new String[args.length - 1];
        for (int i = 0; i < args.length - 1; i++) {
            fileNames[i] = args[i + 1];
        }

        final PrepareConfiguration conf = new PrepareConfiguration.Factory()
                .newPrepareConfiguration(props);
        conf.configure(props);

        MultiThreadPrepareProcessor proc = new MultiThreadPrepareProcessor(conf);
        proc.registerWorkers();
        proc.startWorkers();

        // scan files that are uploaded
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < fileNames.length; i++) {
                    try {
                        com.treasure_data.bulk_import.prepare_parts.Task task =
                                new com.treasure_data.bulk_import.prepare_parts.Task(
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
        List<ErrorInfo> errs = proc.getErrors();
        outputErrors(errs, Configuration.CMD_PREPARE_PARTS);
    }

    public static void uploadParts(final String[] args, Properties props)
            throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("File names not specified");
        }

        String msg = String.format("Start %s command", Configuration.CMD_UPLOAD_PARTS);
        System.out.println(msg);
        LOG.info(msg);

        final String sessionName = args[1];
        final String[] fileNames = new String[args.length - 2];
        for (int i = 0; i < args.length - 2; i++) {
            fileNames[i] = args[i + 2];
        }

        // TODO #MN validate that the session is live or not.

        final UploadConfiguration conf = new UploadConfiguration();
        conf.configure(props);

        MultiThreadUploadProcessor proc = new MultiThreadUploadProcessor(conf);
        proc.registerWorkers();
        proc.startWorkers();

        // scan files that are uploaded
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < fileNames.length; i++) {
                    try {
                        long size = new File(fileNames[i]).length();
                        com.treasure_data.bulk_import.upload_parts.Task task =
                                new com.treasure_data.bulk_import.upload_parts.Task(
                                sessionName, fileNames[i], size);
                        MultiThreadUploadProcessor.addTask(task);
                    } catch (Throwable t) {
                        LOG.severe("Error occurred During 'addTask' method call");
                        LOG.throwing("Main", "addTask", t);
                    }
                }

                // end of file list
                try {
                    MultiThreadUploadProcessor.addFinishTask(conf);
                } catch (Throwable t) {
                    LOG.severe("Error occurred During 'addFinishTask' method call");
                    LOG.throwing("Main", "addFinishTask", t);
                }
            }
        }).start();

        proc.joinWorkers();

        ErrorInfo err = MultiThreadUploadProcessor.processAfterUploading(
                new BulkImportClient(new TreasureDataClient(conf.getProperties())),
                conf, sessionName);

        List<ErrorInfo> errs = proc.getErrors();
        errs.add(err);
        outputErrors(errs, Configuration.CMD_UPLOAD_PARTS);
    }

    public static void prepareAndUploadParts(final String[] args, Properties props)
            throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("File names not specified");
        }

        String msg = String.format("Start %s and %s commands",
                Configuration.CMD_UPLOAD_PARTS,
                Configuration.CMD_PREPARE_PARTS);
        System.out.println(msg);
        LOG.info(msg);

        final String sessionName = args[1];
        final String[] fileNames = new String[args.length - 2];
        for (int i = 0; i < args.length - 2; i++) {
            fileNames[i] = args[i + 2];
        }

        final PrepareConfiguration prepareConf = new PrepareConfiguration.Factory()
                .newPrepareConfiguration(props);
        prepareConf.configure(props);

        MultiThreadPrepareProcessor prepareProc = new MultiThreadPrepareProcessor(prepareConf);
        prepareProc.registerWorkers();
        prepareProc.startWorkers();

        final UploadConfiguration uploadConf = new UploadConfiguration();
        uploadConf.configure(props);

        MultiThreadUploadProcessor uploadProc = new MultiThreadUploadProcessor(uploadConf);
        uploadProc.registerWorkers();
        uploadProc.startWorkers();

        // scan files that are uploaded
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < fileNames.length; i++) {
                    try {
                        UploadTask task =
                                new UploadTask(sessionName, fileNames[i]);
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

        MultiThreadUploadProcessor.addFinishTask(uploadConf);
        uploadProc.joinWorkers();

        ErrorInfo err = MultiThreadUploadProcessor.processAfterUploading(
                new BulkImportClient(new TreasureDataClient(uploadConf.getProperties())),
                uploadConf, sessionName);

        List<ErrorInfo> errs = prepareProc.getErrors();
        errs.addAll(uploadProc.getErrors());
        errs.add(err);
        outputErrors(errs, Configuration.CMD_PREPARE_PARTS + "+"
                + Configuration.CMD_UPLOAD_PARTS);
    }


    private static void outputErrors(List<ErrorInfo> errs, String cmd) {
        int errSize = 0;
        for (ErrorInfo e : errs) {
            if (e.error != null) {
                errSize++;
            }
        }

        if (errSize == 0) {
            return;
        }

        String msg = String.format("Some errors occurred during %s processing. " +
                "Please check the following messages.", cmd);
        System.out.println(msg);
        LOG.warning(msg);

        for (ErrorInfo e : errs) {
            if (e.error != null) {
                e.error.printStackTrace();
            }
        }
    }

    private static boolean includePrepareProcessing(Properties props) {
        for (Iterator<Object> keyIter = props.keySet().iterator(); keyIter.hasNext(); ) {
            String key = (String) keyIter.next();
            if (key.startsWith("td.bulk_import.prepare_parts.")) {
                return true;
            }
        }
        return false;
    }

    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Command not specified");
        }

        String commandName = args[0];
        Properties props = System.getProperties();
        if (commandName.equals(Configuration.CMD_PREPARE_PARTS)) {
            prepareParts(args, props);
        } else if (commandName.equals(Configuration.CMD_UPLOAD_PARTS)) {
            if (!includePrepareProcessing(props)) {
                uploadParts(args, props);
            } else {
                prepareAndUploadParts(args, props);
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("Not support command %s", commandName));
        }
    }

}
