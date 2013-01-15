package com.treasure_data.commands.bulk_import.cellproc;

import java.util.ArrayList;
import java.util.List;

import com.treasure_data.commands.CommandException;

public class CellProcessorGen {
    public CellProcessorGen() {
    }

    public CellProcessor[] gen(String[] columnTypes) throws CommandException {
        int len = columnTypes.length;
        List<CellProcessor> cprocs = new ArrayList<CellProcessor>(len);
        for (int i = 0; i < len; i++) {
            CellProcessor cproc;
            String type = columnTypes[i];
            if (type.equals("string")) {
                cproc = new StringProcessor();
            } else if (type.equals("int")) {
                cproc = new IntProcessor();
            } else if (type.equals("long")) {
                cproc = new LongProcessor();
                // TODO any more...
            } else {
                throw new CommandException("Unsupported type: " + type);
            }
            cprocs.add(cproc);
        }
        return cprocs.toArray(new CellProcessor[0]);
    }
}
