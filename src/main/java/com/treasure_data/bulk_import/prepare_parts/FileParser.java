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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.CharsetDecoder;
import java.util.logging.Logger;

public abstract class FileParser {
    private static final Logger LOG = Logger.getLogger(FileParser.class.getName());

    protected PrepareConfig conf;
    protected long lineNum = 0;
    protected long rowNum = 0;
    protected CharsetDecoder charsetDecoder;
    protected PrepareConfig.CompressionType compressionType;
    protected com.treasure_data.bulk_import.prepare_parts.FileWriter writer;

    private PrintWriter errWriter = null;

    protected FileParser(PrepareConfig conf) {
        this.conf = conf;
    }

    public void configure(String fileName) throws PreparePartsException {
        charsetDecoder = conf.getCharsetDecoder();
        compressionType = conf.checkCompressionType(fileName);
    }

    public void incrementLineNum() {
        lineNum++;
    }

    public long getLineNum() {
        return lineNum;
    }

    public void incrementRowNum() {
        rowNum++;
    }

    public long getRowNum() {
        return rowNum;
    }

    public void setDecorder(CharsetDecoder decorder) {
        this.charsetDecoder = decorder;
    }

    public CharsetDecoder getDecorder() {
        return charsetDecoder;
    }

    public void setFileWriter(com.treasure_data.bulk_import.prepare_parts.FileWriter writer) {
        this.writer = writer;
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

    public abstract void sample(InputStream in) throws PreparePartsException;

    public abstract void parse(InputStream in) throws PreparePartsException;

    public abstract void close() throws PreparePartsException;

    public void closeSilently() {
        try {
            close();
            closeErrorRecordWriter();
        } catch (PreparePartsException e) {
            LOG.severe(e.getMessage());
        }
    }
}
