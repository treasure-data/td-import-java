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
package com.treasure_data.bulk_import.reader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.CharsetDecoder;
import java.util.logging.Logger;

import com.treasure_data.bulk_import.ValueType;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;
import com.treasure_data.bulk_import.prepare_parts.PrepareProcessor;
import com.treasure_data.bulk_import.writer.FileWriter;

public abstract class FileReader {
    private static final Logger LOG = Logger.getLogger(FileReader.class.getName());

    protected PrepareConfiguration conf;
    protected long lineNum = 0;
    protected long rowNum = 0;
    protected CharsetDecoder charsetDecoder;
    protected PrepareConfiguration.CompressionType compressionType;
    protected FileWriter writer;

    private PrintWriter errWriter = null;

    protected FileReader(PrepareConfiguration conf) {
        this.conf = conf;
    }

    public String[] getKeys() {
        return null; // TODO
    }

    public ValueType[] getTypes() {
        return null; // TODO
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

    public void setFileWriter(PrepareProcessor.Task task, FileWriter writer) throws PreparePartsException, IOException {
        writer.setTask(task);
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

    public abstract boolean next() throws PreparePartsException;

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
