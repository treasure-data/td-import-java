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
package com.treasure_data.file;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.CommandRequest;
import com.treasure_data.commands.CommandResult;

public abstract class FileParser<REQ extends CommandRequest, RET extends CommandResult> {
    private static final Logger LOG = Logger.getLogger(FileParser.class
            .getName());

    static final CharsetDecoder UTF8 = Charset.forName("UTF-8")
            .newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);

    protected REQ request;

    private long rowNum = 0;

    private PrintWriter errWriter = null;

    protected FileParser(REQ request) {
        this.request = request;
    }

    public void incrRowNum() {
        rowNum++;
    }

    public long getRowNum() {
        return rowNum;
    }

    public void setErrorRecordWriter(OutputStream errStream) {
        if (errStream != null) {
            errWriter = new PrintWriter(errStream);
        }
    }

    public void writeErrorRecord(String msg) {
        if (errWriter != null) {
            errWriter.println(msg);
        }
    }

    public void closeErrorRecordWriter() {
        if (errWriter != null) {
            errWriter.close();
        }
    }

    public abstract void initParser(CharsetDecoder decoder, InputStream in)
            throws CommandException;

    public abstract void startParsing(final CharsetDecoder decoder, InputStream in)
            throws CommandException;

    public abstract boolean parseRow(com.treasure_data.file.FileWriter w)
            throws CommandException;

    public abstract void close() throws CommandException;

    public void closeSilently() {
        try {
            close();
            closeErrorRecordWriter();
        } catch (CommandException e) {
            LOG.severe(e.getMessage());
        }
    }
}
