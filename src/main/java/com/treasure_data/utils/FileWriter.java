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
import java.util.zip.GZIPOutputStream;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public class FileWriter {
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

    private MessagePack msgpack;
    private Packer packer;
    private GZIPOutputStream gzout;

    private int splitSize;
    private DataSizeChecker dout;
    private int outputFileIndex = 0;
    private String outputDirName;
    private String outputFilePrefix;

    public FileWriter(PreparePartsRequest request, File file)
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

    public void writeRecord(Value[] kvs) throws CommandException {
        try {
            packer.write(ValueFactory.createMapValue(kvs, true));
            if (dout.size() > splitSize) {
                reopenOutputFile();
            }
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

}
