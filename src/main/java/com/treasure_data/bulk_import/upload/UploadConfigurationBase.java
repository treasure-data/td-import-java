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
package com.treasure_data.bulk_import.upload;

import java.util.Properties;

import com.treasure_data.bulk_import.BulkImportOptions;
import com.treasure_data.bulk_import.prepare.PrepareConfiguration;

public class UploadConfigurationBase extends PrepareConfiguration {
    protected int numOfUploadThreads;
    protected int retryCount;
    protected long waitSec;

    public UploadConfigurationBase() {
        super();
    }

    public UploadProcessorBase createNewUploadProcessor() {
        throw new UnsupportedOperationException();
    }

    public void configure(Properties props, BulkImportOptions options) {
        super.configure(props, options);

        // parallel
        setNumOfUploadThreads();

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
        return optionSet.has(BI_PREPARE_PARTS_FORMAT)
                || optionSet.has(BI_PREPARE_PARTS_COMPRESSION)
                || optionSet.has(BI_PREPARE_PARTS_PARALLEL)
                || optionSet.has(BI_PREPARE_PARTS_ENCODING)
                || optionSet.has(BI_PREPARE_PARTS_TIMECOLUMN)
                || optionSet.has(BI_PREPARE_PARTS_TIMEFORMAT)
                || optionSet.has(BI_PREPARE_PARTS_TIMEVALUE)
                || optionSet.has(BI_PREPARE_PARTS_OUTPUTDIR)
                || optionSet.has(BI_PREPARE_PARTS_ERROR_RECORDS_HANDLING)
                || optionSet.has("dry-run")
                || optionSet.has(BI_PREPARE_PARTS_SPLIT_SIZE)
                || optionSet.has(BI_PREPARE_PARTS_COLUMNS)
                || optionSet.has(BI_PREPARE_PARTS_COLUMNTYPES)
                || optionSet.has(BI_PREPARE_PARTS_EXCLUDE_COLUMNS)
                || optionSet.has(BI_PREPARE_PARTS_ONLY_COLUMNS);
    }

    public void setNumOfUploadThreads() {
        numOfUploadThreads = 2;
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

}