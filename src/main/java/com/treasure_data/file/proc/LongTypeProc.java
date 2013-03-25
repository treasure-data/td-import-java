package com.treasure_data.file.proc;

import com.treasure_data.commands.CommandException;
import com.treasure_data.file.FileWriter;

public class LongTypeProc implements ColumnProc<Long> {

    private FileWriter w;

    public LongTypeProc(FileWriter w) {
        this.w = w;
    }

    @Override
    public void execute(Long value) {
        if (value != null) {
            try {
                w.writeLong(value);
            } catch (CommandException e) {
                // TODO
            }
        } else {
            try {
                w.writeNil();
            } catch (CommandException e) {
                // TODO
            }
        }
    }

}
