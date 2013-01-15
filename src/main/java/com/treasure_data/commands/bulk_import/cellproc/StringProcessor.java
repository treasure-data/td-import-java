package com.treasure_data.commands.bulk_import.cellproc;

import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;

public class StringProcessor implements CellProcessor {
    private static Value NIL = ValueFactory.createNilValue();

    public Value doIt(String text) {
        if (text == null || text.isEmpty()) {
            return NIL;
        }
        // TODO need validation
        return ValueFactory.createRawValue(text);
    }
}
