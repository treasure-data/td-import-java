package com.treasure_data.bulk_import.prepare_parts;


public class ErrorInfo {
    public Task task;
    public Throwable error = null;

    public long redLines = 0;
    public long redRows = 0;
    public long writtenRows = 0;

    @Override
    public String toString() {
        return String.format(
                "prepare_error_info{task=%s, redLines=%d, redRows=%d, writtenRows=%d}",
                task, redLines, redRows, writtenRows);
    }
}