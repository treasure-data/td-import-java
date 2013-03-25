package com.treasure_data.file.proc;

import org.supercsv.cellprocessor.ift.CellProcessor;

import com.treasure_data.commands.bulk_import.CSVPreparePartsRequest;
import com.treasure_data.file.TimeFormatSuggestionProcessor.TimeFormatProcessor;

public class ColumnProcessorGen {

    public ColumnProcessorGen() {
    }

    public CellProcessor[] gen(CSVPreparePartsRequest.ColumnType[] columnTypes,
            TimeFormatProcessor timeFormatProc) {
        return null;
    }
}
