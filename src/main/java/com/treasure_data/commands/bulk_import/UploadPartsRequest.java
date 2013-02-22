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

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.CommandRequest;
import com.treasure_data.commands.Config;

public class UploadPartsRequest extends CommandRequest {
    private static final Logger LOG = Logger
            .getLogger(UploadPartsRequest.class.getName());

    private static final String COMMAND_NAME = "upload_parts";

    protected String sessionName;
    protected boolean autoPerform;
    protected boolean autoCommit;
    protected int retryCount;
    protected long waitSec;

    //bulk_import:upload <name> <raw file paths…> [--auto-perform=true] [--auto-commit=false] [PREPARE_PARTS_OPTIONS]
    public UploadPartsRequest() throws CommandException {
        super(null);
    }

    public UploadPartsRequest(String sessionName, String[] fileNames, Properties props) throws CommandException {
        super(props);
        setSessionName(sessionName);
        setFiles(fileNames);
        setOptions(props);
    }

    @Override
    protected String getName() {
        return COMMAND_NAME;
    }

    protected void setSessionName(String sessionName) {
        this.sessionName = sessionName;
    }

    protected String getSessionName() {
        return sessionName;
    }

    protected void setOptions(Properties props) throws CommandException {
        // auto-perform
        String aperform = props.getProperty(Config.BI_UPLOAD_PARTS_AUTO_PERFORM,
                Config.BI_UPLOAD_PARTS_AUTO_PERFORM_DEFAULTVALUE);
        autoPerform = aperform != null && aperform.equals("true");

        // auto-commit
        String acommit = props.getProperty(Config.BI_UPLOAD_PARTS_AUTO_COMMIT,
                Config.BI_UPLOAD_PARTS_AUTO_COMMIT_DEFAULTVALUE);
        autoCommit = aperform != null && acommit.equals("true");

        // retryCount
        String rcount = props.getProperty(Config.BI_UPLOAD_PARTS_RETRYCOUNT,
                Config.BI_UPLOAD_PARTS_RETRYCOUNT_DEFAULTVALUE);
        try {
            retryCount = Integer.parseInt(rcount);
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "retry count is required as int type e.g. -D%s=5",
                    Config.BI_UPLOAD_PARTS_RETRYCOUNT);
            throw new CommandException(msg, e);
        }

        // waitSec
        String wsec = props.getProperty(Config.BI_UPLOAD_PARTS_WAITSEC,
                Config.BI_UPLOAD_PARTS_WAITSEC_DEFAULTVALUE);
        try {
            waitSec = Long.parseLong(wsec);
        } catch (NumberFormatException e) {
            String msg = String.format(
                    "wait sec is required as long type e.g. -D%s=5",
                    Config.BI_UPLOAD_PARTS_WAITSEC);
            throw new CommandException(msg, e);
        }
    }

    public boolean autoPerform() {
        return autoPerform;
    }

    public boolean autoCommit() {
        return autoCommit;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getWaitSec() {
        return waitSec;
    }
}
