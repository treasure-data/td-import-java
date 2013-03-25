package com.treasure_data.file.proc;

public interface ColumnProc<T> {
    void execute(final T value);
}
