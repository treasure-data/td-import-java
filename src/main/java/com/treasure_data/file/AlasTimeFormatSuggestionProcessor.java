package com.treasure_data.file;

import org.supercsv.util.CsvContext;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.CSVPreparePartsRequest.ColumnType;
import com.treasure_data.file.TimeFormatSuggestionProcessor.TimeFormat;
import com.treasure_data.file.TimeFormatSuggestionProcessor.TimeFormatProcessor;

public class AlasTimeFormatSuggestionProcessor extends TypeSuggestionProcessor {

    private TimeFormatSuggestionProcessor timeFormatProcessor;

    AlasTimeFormatSuggestionProcessor(int rowSize, int hintScore) {
        super(rowSize, hintScore);
        timeFormatProcessor = new TimeFormatSuggestionProcessor(rowSize, hintScore);
    }

    @Override
    void addHint(String typeHint) throws CommandException {
        addHint(typeHint);
    }

    @Override
    ColumnType getSuggestedType() {
        return super.getSuggestedType();
    }

    TimeFormatProcessor getSuggestedTimeFormatProcessor()
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
