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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.treasure_data.commands.Command;
import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest.CompressionType;
import com.treasure_data.file.FileParser;
import com.treasure_data.file.FileParserFactory;
import com.treasure_data.file.MsgpackGZIPFileWriter;

public class PreparePartsCommand extends
        Command<PreparePartsRequest, PreparePartsResult> {
    private static final Logger LOG = Logger
            .getLogger(PreparePartsCommand.class.getName());

    public PreparePartsCommand() {
    }

    @Override
    public void execute(PreparePartsRequest request, PreparePartsResult result,
            final File file) throws CommandException {
        LOG.fine(String.format("started converting file: %s", file.getName()));

        // TODO #MN need type paramters
        FileParser<?, ?> p = null;
        MsgpackGZIPFileWriter w = null;
        try {
            CompressionType compressionType = getCompressType(request, file);
            CharsetDecoder decoder = getCharsetDecoder(request);

            p = FileParserFactory.newInstance(request, result);
            p.initParser(decoder, createFileInputStream(compressionType, file));

            if (request.dryRun()) {
                // if this processing is dry-run mode, thread of control
                // returns back
                return;
            }

            p.setErrorRecordWriter(createErrorRecordOutputStream(request,
                    file.getName()));
            p.startParsing(decoder,
                    createFileInputStream(compressionType, file));
            w = new MsgpackGZIPFileWriter(request, result);
            w.initWriter(file.getName());
            while (p.parseRow(w)) {
                ;
            }
        } finally {
            if (p != null) {
                p.closeSilently();
            }
            if (w != null) {
                w.closeSilently();
            }
        }

        result.setParsedRowNum(p.getRowNum());
        result.setWrittenRowNum(w.getRowNum());

        LOG.info(String.format("file: %s: %d entries", file.getName(),
                result.getParsedRowNum()));
    }

    private static OutputStream createErrorRecordOutputStream(
            PreparePartsRequest request, String infileName)
            throws CommandException {
        // outputDir
        String outputDirName = request.getErrorRecordOutputDirName();
        if (outputDirName == null) {
            return null;
        }

        // outputFilePrefix
        int lastSepIndex = infileName.lastIndexOf(File.pathSeparator);
        String outputFilePrefix = infileName.substring(lastSepIndex + 1,
                infileName.length()).replace('.', '_');
        String outputFileName = outputFilePrefix + ".err.txt";

        try {
            File outputFile = new File(outputDirName, outputFileName);
            LOG.fine("Created output file: " + outputFileName);
            return new BufferedOutputStream(new FileOutputStream(outputFile));
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    private static InputStream createFileInputStream(
            CompressionType compressionType, final File infile)
            throws CommandException {
        try {
            if (compressionType.equals(CompressionType.GZIP)) {
                return new GZIPInputStream(new FileInputStream(infile));
            } else if (compressionType.equals(CompressionType.NONE)) {
                return new FileInputStream(infile);
            } else {
                throw new CommandException("unsupported compress type: "
                        + compressionType);
            }
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    private static CompressionType getCompressType(PreparePartsRequest request,
            final File infile) throws CommandException {
        String fileName = infile.getName();
        CompressionType userCompressType = request.getCompressionType();
        if (userCompressType == null) {
            throw new CommandException("fatal error");
        }

        CompressionType[] candidateCompressTypes;
        if (userCompressType.equals(CompressionType.GZIP)) {
            candidateCompressTypes = new CompressionType[] { CompressionType.GZIP, };
        } else if (userCompressType.equals(CompressionType.NONE)) {
            candidateCompressTypes = new CompressionType[] { CompressionType.NONE, };
        } else if (userCompressType.equals(CompressionType.AUTO)) {
            candidateCompressTypes = new CompressionType[] {
                    CompressionType.GZIP, CompressionType.NONE, };
        } else {
            throw new CommandException("unsupported compression type: "
                    + userCompressType);
        }

        CompressionType compressionType = null;
        for (int i = 0; i < candidateCompressTypes.length; i++) {
            InputStream in = null;
            try {
                if (candidateCompressTypes[i].equals(CompressionType.GZIP)) {
                    in = new GZIPInputStream(new FileInputStream(fileName));
                } else if (candidateCompressTypes[i]
                        .equals(CompressionType.NONE)) {
                    in = new FileInputStream(fileName);
                } else {
                    throw new CommandException("fatal error");
                }
                byte[] b = new byte[2];
                in.read(b);

                compressionType = candidateCompressTypes[i];
                break;
            } catch (IOException e) {
                LOG.fine(String.format("file %s is %s", fileName,
                        e.getMessage()));
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }

        if (compressionType == null) {
            throw new CommandException(new IOException(String.format(
                    "cannot read file %s with specified compress type: %s",
                    fileName, userCompressType)));
        }

        return compressionType;
    }

    private static CharsetDecoder getCharsetDecoder(
            PreparePartsRequest request) throws CommandException {
        // encoding
        String encodingName = request.getEncoding();
        if (encodingName.equals("utf-8")) {
            return Charset.forName("UTF-8").newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
        } else {
            // TODO any more...
            throw new CommandException(new UnsupportedOperationException());
        }
    }
}
