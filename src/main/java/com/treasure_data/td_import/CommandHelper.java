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
import java.util.ArrayList;
import java.util.List;

public class CommandHelper {
    public CommandHelper() {
    }

    public void showPrepare(String[] fileNames, String outputDirName) {
        System.out.println();
        System.out.println("Preparing files");
        System.out.println(String.format("  Output dir   : %s", outputDirName));
        showFiles(fileNames);
        System.out.println();
    }

    public void showUpload(String[] fileNames, String sessionName) {
        System.out.println();
        System.out.println("Uploading prepared files");
        System.out.println(String.format("  Session name : %s", sessionName));
        showFiles(fileNames);
        System.out.println();
    }

    protected void showFiles(String[] fileNames) {
        for (String fileName : fileNames) {
            System.out.println(String.format("  File       : %s (%d bytes)", fileName, new File(fileName).length()));
        }
    }

    public void showPrepareResults(List<com.treasure_data.td_import.prepare.TaskResult> results) {
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

    public void listNextStepOfPrepareProc(List<com.treasure_data.td_import.prepare.TaskResult> results) {
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

    public void showUploadResults(List<com.treasure_data.td_import.upload.TaskResult> results) {
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

    public void listNextStepOfUploadProc(List<com.treasure_data.td_import.upload.TaskResult> results,
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

}
