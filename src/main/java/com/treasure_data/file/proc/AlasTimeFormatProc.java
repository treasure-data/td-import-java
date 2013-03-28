package com.treasure_data.file.proc;

import org.supercsv.util.CsvContext;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.CSVPreparePartsRequest.ColumnType;
import com.treasure_data.file.proc.TimeFormatSuggestProc.TimeFormat;
import com.treasure_data.file.proc.TimeFormatSuggestProc.TimeFormatProcessor;

public class AlasTimeFormatProc extends TypeSuggestProc {

    private TimeFormatSuggestProc timeFormatProcessor;

    public AlasTimeFormatProc(int rowSize) {
        super(rowSize);
        timeFormatProcessor = new TimeFormatSuggestProc(rowSize);
    }

    @Override
    public ColumnType getSuggestedType() {
        return super.getSuggestedType();
    }

    public TimeFormatProcessor getSuggestedTimeFormatProcessor()
            throws CommandException {
        return timeFormatProcessor
                .createTimeFormatProcessor(timeFormatProcessor
                        .getSuggestedTimeFormat());
    }

    TimeFormat getSuggestedTimeFormat() {
        return timeFormatProcessor.getSuggestedTimeFormat();
    }

    TimeFormatProcessor createTimeFormatProcessor(TimeFormat tf)
            throws CommandException {
        return timeFormatProcessor.createTimeFormatProcessor(tf);
    }

    @Override
    public Object execute(Object value, CsvContext context) {
        timeFormatProcessor.execute(value, context);
        return super.execute(value, context);
    }
}
