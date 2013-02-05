package com.treasure_data.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public class ErrorRecordFileWriter {
    private static final Logger LOG = Logger
            .getLogger(ErrorRecordFileWriter.class.getName());

    protected OutputStream out;

    public ErrorRecordFileWriter(PreparePartsRequest request, String infileName)
            throws CommandException {
        initWriter(request, infileName);
    }

    public void initWriter(PreparePartsRequest request, String infileName)
            throws CommandException {
        // outputFilePrefix
        int lastSepIndex = infileName.lastIndexOf(File.pathSeparator);
        String outputFilePrefix = infileName.substring(lastSepIndex + 1,
                infileName.length()).replace('.', '_');
        String outputFileName = outputFilePrefix + ".err.txt";

        // outputDir
        String outputDirName = request.getErrorRecordOutputDirName();
        if (outputDirName != null) {
            try {
                out = new BufferedOutputStream(new FileOutputStream(new File(
                        outputDirName, outputFileName)));
            } catch (IOException e) {
                throw new CommandException(e);
            }
            LOG.info("Created output file: " + outputFileName);
        }
    }

    public void writeErrorRow(String message, int rowNumber, String row)
            throws CommandException {
        // TODO #MN should create NullWriter class
        if (out == null) {
            return;
        }

        // TODO
        // TODO
    }

    public void close() throws CommandException {
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                throw new CommandException(e);
            }
        }
        out = null;
    }
}
