package com.treasure_data.file.proc;

import com.treasure_data.commands.CommandException;
import com.treasure_data.file.FileWriter;

public class StringTypeProc implements ColumnProc<String> {

    private FileWriter w;

    public StringTypeProc(FileWriter w) {
        this.w = w;
    }

    @Override
    public void execute(String value) {
        if (value != null) {
            try {
                w.writeString(value);
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
