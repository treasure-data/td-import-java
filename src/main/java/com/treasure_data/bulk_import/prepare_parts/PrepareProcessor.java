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
package com.treasure_data.bulk_import.prepare_parts;

import java.util.logging.Logger;

public class PrepareProcessor {

    public static class Task {
        String fileName;

        public Task(String fileName) {
            this.fileName = fileName;
        }
    }

    public static class ErrorInfo {
        Task task;
        Throwable error;

        long redRows;
        long writtenRows;

        public ErrorInfo(Task task, Throwable error, long redRows, long writtenRows) {
            this.task = task;
            this.error = error;
            this.redRows = redRows;
            this.writtenRows = writtenRows;
        }
    }

    private static final Logger LOG = Logger.getLogger(
            PrepareProcessor.class.getName());

    protected PrepareConfig conf;

    public PrepareProcessor(PrepareConfig conf) {
        this.conf = conf;
    }

    public ErrorInfo execute(final Task task) {
        LOG.info(String.format("Convert file '%s'", task.fileName));

        // TODO #MN need type paramters
        ErrorInfo err = null;
        FileParser p = null;
        MsgpackGZIPFileWriter writer = null;
        try {
            p = FileParser.newFileParser(conf);
            p.configure(task.fileName);
            p.sample(conf.createFileInputStream(task.fileName));

//            if (conf.dryRun()) {
//                // if this processing is dry-run mode, thread of control
//                // returns back
//                return new ErrorInfo(task, null, 0, 0);
//            }

            writer = new MsgpackGZIPFileWriter(conf);
            writer.initWriter(task.fileName);

            p.setFileWriter(writer);
            p.parse(conf.createFileInputStream(task.fileName));
        } catch (PreparePartsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (p != null) {
                p.closeSilently();
            }
            if (writer != null) {
                writer.closeSilently();
            }
        }

        err.redRows = p.getRowNum();
        err.writtenRows = writer.getRowNum();

        LOG.info(String.format("Converted file '%s', %d entries",
                task.fileName, err.writtenRows));
        return err;
    }

}
