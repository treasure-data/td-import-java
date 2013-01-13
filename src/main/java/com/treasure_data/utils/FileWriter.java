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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.template.Templates;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public class FileWriter {
    static class DataSizeChecker extends FilterOutputStream {

        private int size = 0;

        public DataSizeChecker(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            System.out.println("# write(int)");
            size++;
            super.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            System.out.println("# write(byte[], int, int)");
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
                infileName.length());

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
            String outputFileName = outputFilePrefix + "_" + outputFileIndex + ".msgpack.gz";
            outputFileIndex++;
            dout = new DataSizeChecker(new FileOutputStream(new File(outputDirName, outputFileName)));
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

    public void writeRecord(Map<String, Object> record) throws CommandException {
        try {
            packer.writeMapBegin(record.size());
            for (Map.Entry<String, Object> pair : record.entrySet()) {
                Templates.TString.write(packer, pair.getKey());
                packer.write(pair.getValue());
            }
            packer.writeMapEnd();

            System.out.println(dout.size()); // TODO
            if (dout.size() > splitSize) {
                reopenOutputFile();
            }
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

}
