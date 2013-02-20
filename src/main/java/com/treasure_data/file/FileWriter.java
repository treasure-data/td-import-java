package com.treasure_data.file;

import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.CommandRequest;
import com.treasure_data.commands.CommandResult;

public abstract class FileWriter<REQ extends CommandRequest, RET extends CommandResult>
        implements Closeable {
    private static final Logger LOG = Logger
            .getLogger(FileWriter.class.getName());

    protected REQ request;
    protected RET result;
    protected long rowNum = 0;

    protected FileWriter(REQ request, RET result) throws CommandException {
        this.request = request;
        this.result = result;
    }

    protected abstract void initWriter(String infileName)
            throws CommandException;

    protected abstract void writeBeginRow(int size) throws CommandException;

    protected abstract void write(Object o) throws CommandException;

    protected abstract void writeEndRow() throws CommandException;

    public void incrRowNum() {
        rowNum++;
    }

    public long getRowNum() {
        return rowNum;
    }

    public abstract void close() throws IOException;

    public void closeSilently() {
        try {
            close();
        } catch (IOException e) {
            LOG.severe(e.getMessage());
        }
    }
}
