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
package com.treasure_data.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public class MsgpackGZIPFileWriter {
    static class DataSizeChecker extends FilterOutputStream {

        private int size = 0;

        public DataSizeChecker(OutputStream out) {
            super(out);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            size += len;
            super.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            size = 0; // Refresh
            super.close();
        }

        public int size() {
            return size;
        }
    }

    private static final Logger LOG = Logger
            .getLogger(MsgpackGZIPFileWriter.class.getName());

    private MessagePack msgpack;
    private Packer packer;
    private GZIPOutputStream gzout;

    private int splitSize;
    private DataSizeChecker dout;
    private int outputFileIndex = 0;
    private String outputDirName;
    private String outputFilePrefix;

    public MsgpackGZIPFileWriter(PreparePartsRequest request, File file)
            throws CommandException {
        initWriter(request, file);
    }

    public void initWriter(PreparePartsRequest request, File infile)
            throws CommandException {
        msgpack = new MessagePack();

        // outputFilePrefix
        String infileName = infile.getName();
        int lastSepIndex = infileName.lastIndexOf(File.pathSeparator);
        outputFilePrefix = infileName.substring(lastSepIndex + 1,
                infileName.length()).replace('.', '_');

        // outputDir
        outputDirName = request.getOutputDirName();

        // splitSize
        splitSize = request.getSplitSize() * 1024;

        reopenOutputFile();
    }

    private void reopenOutputFile() throws CommandException {
        // close stream
        if (outputFileIndex != 0) {
            close();
        }

        // create msgpack packer
        try {
            String outputFileName = outputFilePrefix + "_" + outputFileIndex
                    + ".msgpack.gz";
            outputFileIndex++;
            dout = new DataSizeChecker(new BufferedOutputStream(
                    new FileOutputStream(new File(outputDirName, outputFileName))));
            gzout = new GZIPOutputStream(dout);
            packer = msgpack.createPacker(gzout);

            LOG.info("Created output file: " + outputFileName);
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    public void writeBeginRow(int size) throws CommandException {
        try {
            packer.writeMapBegin(size);
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    public void write(Object o) throws CommandException {
        try {
            packer.write(o);
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    public void write(long o) throws CommandException {
        try {
            packer.write(o);
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    public void writeEndRow() throws CommandException {
        try {
            packer.writeMapEnd();
            if (dout.size() > splitSize) {
                reopenOutputFile();
            }
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    public void close() throws CommandException {
        if (gzout != null) {
            try {
                gzout.close();
            } catch (IOException e) {
                throw new CommandException(e);
            }
            gzout = null;
            dout = null;
        }
        packer = null;
    }

    public void closeSilently() {
        try {
            close();
        } catch (CommandException e) {
            LOG.severe(e.getMessage());
        }
    }
}
