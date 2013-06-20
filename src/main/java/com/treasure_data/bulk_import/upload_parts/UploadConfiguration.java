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

import java.util.Properties;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;

public class UploadConfiguration extends PrepareConfiguration {

    protected boolean autoPerform;
    protected boolean autoCommit;
    protected int numOfUploadThreads;
    protected int retryCount;
    protected long waitSec;

    public UploadConfiguration() {
        super();
    }

    public void configure(Properties props) {
        super.configure(props);

        // auto-perform
        String aperform = props.getProperty(BI_UPLOAD_PARTS_AUTO_PERFORM,
                BI_UPLOAD_PARTS_AUTO_PERFORM_DEFAULTVALUE);
        autoPerform = aperform != null && aperform.equals("true");

        // auto-commit
        String acommit = props.getProperty(BI_UPLOAD_PARTS_AUTO_COMMIT,
                BI_UPLOAD_PARTS_AUTO_COMMIT_DEFAULTVALUE);
        autoCommit = aperform != null && acommit.equals("true");

        // parallel
        String uthreadNum = props.getProperty(BI_UPLOAD_PARTS_PARALLEL,
                BI_UPLOAD_PARTS_PARALLEL_DEFAULTVALUE);
        try {
            int n = Integer.parseInt(uthreadNum);
            if (n < 0) {
                numOfUploadThreads = 2;
            } else if (n > 9){
                numOfUploadThreads = 8;
            } else {
                numOfUploadThreads = n;
            }
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "'int' value is required as 'parallel' option e.g. -D%s=5",
                    BI_UPLOAD_PARTS_PARALLEL);
            throw new IllegalArgumentException(msg, e);
        }

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

    public boolean autoPerform() {
        return autoPerform;
    }

    public boolean autoCommit() {
        return autoCommit;
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