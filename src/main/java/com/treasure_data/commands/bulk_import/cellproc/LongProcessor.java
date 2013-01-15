package com.treasure_data.commands.bulk_import.cellproc;

import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;

public class LongProcessor implements CellProcessor {
    private static Value ZERO = ValueFactory.createIntegerValue(0L);

    public Value doIt(String text) {
        if (text == null || text.isEmpty()) {
            return ZERO;
        }
        return ValueFactory.createIntegerValue(Long.parseLong(text));
    }
}
