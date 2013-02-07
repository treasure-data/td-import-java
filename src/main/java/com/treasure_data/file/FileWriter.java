package com.treasure_data.file;

import java.io.Closeable;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.CommandRequest;
import com.treasure_data.commands.CommandResult;

public abstract class FileWriter<REQ extends CommandRequest, RET extends CommandResult>
        implements Closeable {

    protected REQ request;

    protected FileWriter(REQ request) throws CommandException {
        this.request = request;
    }

    protected abstract void initWriter(String infileName)
            throws CommandException;

    protected abstract void writeBeginRow(int size) throws CommandException;

    protected abstract void write(Object o) throws CommandException;

    protected abstract void writeEndRow() throws CommandException;
}
