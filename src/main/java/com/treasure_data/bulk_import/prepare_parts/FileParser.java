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
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.logging.Logger;

public abstract class FileParser {
    // TODO #MN should consider type parameters
    public static FileParser newInstance(PrepareConfig conf) throws PreparePartsException {
        PrepareConfig.Format format = conf.getFormat();
        if (format.equals(PrepareConfig.Format.CSV)
                || format.equals(PrepareConfig.Format.TSV)) {
            return new CSVFileParser(conf);
        } else if (format.equals(PrepareConfig.Format.JSON)) {
            //return new JSONFileParser(request, result);
            throw new PreparePartsException(new UnsupportedOperationException(
                    "format: " + format));
        } else if (format.equals(PrepareConfig.Format.MSGPACK)) {
            throw new PreparePartsException(new UnsupportedOperationException(
                    "format: " + format));
        } else {
            throw new PreparePartsException("Invalid format: " + format);
        }
    }

    private static final Logger LOG = Logger.getLogger(FileParser.class
            .getName());

    private static final CharsetDecoder UTF_8 = Charset.forName("UTF-8")
            .newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);

    protected PrepareConfig conf;
    protected long rowNum = 0;
    protected PrintWriter errWriter = null;

    protected FileParser(PrepareConfig conf) {
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
            throws PreparePartsException;

    public abstract void startParsing(final CharsetDecoder decoder, InputStream in)
            throws PreparePartsException;

    public abstract boolean parseRow(
            com.treasure_data.bulk_import.prepare_parts.MsgpackGZIPFileWriter w)
            throws PreparePartsException;

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
