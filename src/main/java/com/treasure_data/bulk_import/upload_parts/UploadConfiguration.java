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
package com.treasure_data.bulk_import.upload_parts;

import java.util.List;
import java.util.Properties;

import joptsimple.OptionSet;

import com.treasure_data.bulk_import.BulkImportOptions;
import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;

public class UploadConfiguration extends PrepareConfiguration {

    public static class Factory {
        protected BulkImportOptions options;

        public Factory(Properties props) {
            options = new BulkImportOptions();
            options.initUploadOptionParser(props);
        }

        public BulkImportOptions getBulkImportOptions() {
            return options;
        }

        public UploadConfiguration newUploadConfiguration(String[] args) {
            options.setOptions(args);
            OptionSet optionSet = options.getOptions();

//            // TODO FIXME when uploadParts is called, default format is "msgpack.gz"
//            // on the other hand, when prepareParts, default format is "csv".
//            String formatStr;
//            if (optionSet.has("format")) {
//                formatStr = (String) optionSet.valueOf("format");
//            } else {
//                formatStr = Configuration.BI_UPLOAD_PARTS_FORMAT_DEFAULTVALUE;
//            }
//
//            // lookup format enum
//            Format format = Format.fromString(formatStr);
//            if (format == null) {
//                throw new IllegalArgumentException(String.format(
//                        "unsupported format '%s'", formatStr));
//            }
            return new UploadConfiguration();
        }
    }

    protected boolean autoPerform;
    protected boolean autoCommit;
    protected boolean autoCreateSession;
    protected boolean autoDeleteSession;
    protected int numOfUploadThreads;
    protected int retryCount;
    protected long waitSec;

    public UploadConfiguration() {
        super();
    }

    // TODO FIXME #MN re-implement the method
    public void configure(Properties props, BulkImportOptions options) {
        super.configure(props, options);

        // auto-perform
        setAutoPerform();

        // auto-commit
        setAutoCommit();

        // parallel
        setNumOfUploadThreads();

        // auto-create-session
        setAutoCreateSession();

        // auto-delete-session
        setAutoDeleteSession();

        // retryCount
        String rcount = props.getProperty(BI_UPLOAD_PARTS_RETRYCOUNT,
                BI_UPLOAD_PARTS_RETRYCOUNT_DEFAULTVALUE);
        try {
            retryCount = Integer.parseInt(rcount);
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "'int' value is required as 'retry count' option e.g. -D%s=5",
                    BI_UPLOAD_PARTS_RETRYCOUNT);
            throw new IllegalArgumentException(msg, e);
        }

        // waitSec
        String wsec = props.getProperty(BI_UPLOAD_PARTS_WAITSEC,
                BI_UPLOAD_PARTS_WAITSEC_DEFAULTVALUE);
        try {
            waitSec = Long.parseLong(wsec);
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "'long' value is required as 'wait sec' e.g. -D%s=5",
                    BI_UPLOAD_PARTS_WAITSEC);
            throw new IllegalArgumentException(msg, e);
        }
    }

    public Properties getProperties() {
        return props;
    }

    public boolean hasPrepareOptions() {
        return optionSet.has(Configuration.BI_PREPARE_PARTS_FORMAT)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_COMPRESSION)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_PARALLEL)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_ENCODING)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_TIMECOLUMN)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_TIMEFORMAT)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_TIMEVALUE)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_OUTPUTDIR)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_ERROR_RECORDS_HANDLING)
                || optionSet.has("dry-run")
                || optionSet.has(Configuration.BI_PREPARE_PARTS_SPLIT_SIZE)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_COLUMNS)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_COLUMNTYPES)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_EXCLUDE_COLUMNS)
                || optionSet.has(Configuration.BI_PREPARE_PARTS_ONLY_COLUMNS);
    }

    public void setAutoPerform() {
        autoPerform = optionSet.has(
                Configuration.BI_UPLOAD_PARTS_AUTO_PERFORM);
    }

    public boolean autoPerform() {
        return autoPerform;
    }

    public void setAutoCommit() {
        autoCommit = optionSet.has(
                Configuration.BI_UPLOAD_PARTS_AUTO_COMMIT);
    }

    public boolean autoCommit() {
        return autoCommit;
    }

    public void setAutoCreateSession() {
        autoCreateSession = optionSet.has(
                Configuration.BI_UPLOAD_PARTS_AUTO_CREATE_SESSION);
    }

    public boolean autoCreateSession() {
        return autoCreateSession;
    }

    public void setAutoDeleteSession() {
        autoDeleteSession = optionSet.has(
                Configuration.BI_UPLOAD_PARTS_AUTO_DELETE_SESSION);
    }

    public boolean autoDeleteSession() {
        return autoDeleteSession;
    }

    public void setNumOfUploadThreads() {
        String num;
        if (!optionSet.has("parallel")) {
            num = Configuration.BI_UPLOAD_PARTS_PARALLEL_DEFAULTVALUE;
        } else {
            num = (String) optionSet.valueOf(
                    Configuration.BI_UPLOAD_PARTS_PARALLEL);
        }

        try {
            int n = Integer.parseInt(num);
            if (n < 0) {
                numOfUploadThreads = 2;
            } else if (n > 9){
                numOfUploadThreads = 8;
            } else {
                numOfUploadThreads = n;
            }
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "'int' value is required as 'parallel' option");
            throw new IllegalArgumentException(msg, e);
        }
    }

    public int getNumOfUploadThreads() {
        return numOfUploadThreads;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getWaitSec() {
        return waitSec;
    }

    public Object clone() {
        UploadConfiguration conf = new UploadConfiguration();
        conf.props = props;
        conf.autoPerform = autoPerform;
        conf.autoCommit = autoCommit;
        conf.numOfUploadThreads = numOfUploadThreads;
        conf.retryCount= retryCount;
        conf.waitSec = waitSec;
        return conf;
    }
}
