package com.treasure_data.commands.bulk_import.cellproc;

import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;

public class IntProcessor implements CellProcessor {
    private static Value ZERO = ValueFactory.createIntegerValue(0);

    public Value doIt(String text) {
        if (text == null || text.isEmpty()) {
            return ZERO;
        }
        return ValueFactory.createIntegerValue(Integer.parseInt(text));
    }
}
