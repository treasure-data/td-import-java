package com.treasure_data.file.proc;

import com.treasure_data.commands.CommandException;
import com.treasure_data.file.FileWriter;

public class IntTypeProc implements ColumnProc<Integer> {

    private FileWriter w;

    public IntTypeProc(FileWriter w) {
        this.w = w;
    }

    @Override
    public void execute(Integer value) {
        if (value != null) {
            try {
                w.writeInt(value);
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
