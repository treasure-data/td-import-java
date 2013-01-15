package com.treasure_data.commands.bulk_import.cellproc;

import org.msgpack.type.Value;

public interface CellProcessor {
    Value doIt(String text);
}
