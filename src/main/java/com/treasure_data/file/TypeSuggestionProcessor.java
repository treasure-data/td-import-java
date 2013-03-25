package com.treasure_data.file;

import static com.treasure_data.commands.bulk_import.CSVPreparePartsRequest.ColumnType.DOUBLE;
import static com.treasure_data.commands.bulk_import.CSVPreparePartsRequest.ColumnType.INT;
import static com.treasure_data.commands.bulk_import.CSVPreparePartsRequest.ColumnType.LONG;
import static com.treasure_data.commands.bulk_import.CSVPreparePartsRequest.ColumnType.STRING;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.util.CsvContext;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.CSVPreparePartsRequest;
import com.treasure_data.commands.bulk_import.CSVPreparePartsRequest.ColumnType;

public class TypeSuggestionProcessor extends CellProcessorAdaptor {
    private boolean fixedColumnType = false;
    private CSVPreparePartsRequest.ColumnType columnType;

    private int[] scores = new int[] { 0, 0, 0, 0 };
    protected int rowSize;

    TypeSuggestionProcessor(int rowSize) {
        this.rowSize = rowSize;
    }

    void setType(String columnType) throws CommandException {
        if (columnType == null) {
            throw new NullPointerException("column type is null.");
        }

        //CSVPreparePartsRequest.ColumnType type = ColumnType.fromString(columnType);
        this.columnType = ColumnType.fromString(columnType);
        if (this.columnType == null) { // fatal error
            throw new CommandException(String.format(
                    "specified column type is not supported: %s",
                    columnType));
        }
        fixedColumnType = true;
    }

    ColumnType getSuggestedType() {
        if (fixedColumnType) {
            return this.columnType;
        }

        int max = -rowSize;
        int maxIndex = 0;
        for (int i = 0; i < scores.length; i++) {
            if (max < scores[i]) {
                max = scores[i];
                maxIndex = i;
            }
        }
        return ColumnType.fromInt(maxIndex);
    }

    int getScore(ColumnType type) {
        int i = type.index();
        if (i < 0 || i >= 4) {
            throw new ArrayIndexOutOfBoundsException(i);
        }
        return scores[i];
    }

    @Override
    public Object execute(Object value, CsvContext context) {
        if (fixedColumnType) {
            return next.execute(value, context);
        }

        if (value == null) {
            // any score are not changed
            return null;
        }

        Object result = null;

        // value looks like String object?
        if (value instanceof String) {
            scores[STRING.index()] += 1;
            result = (String) value;
        }

        // value looks like Double object?
        if (value instanceof Double) {
            result = (Double) value;
            scores[DOUBLE.index()] += 1;
        } else if (value instanceof String) {
            try {
                result = Double.parseDouble((String) value);
                scores[DOUBLE.index()] += 1;
            } catch (NumberFormatException e) {
                // ignore
            }
        }


        if (value instanceof Long) {
            result = (Long) value;
            scores[LONG.index()] += 1;
        } else if (value instanceof String) {
            try {
                result = Long.parseLong((String) value);
                scores[LONG.index()] += 1;
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        // value looks like Integer object?
        if (value instanceof Integer) {
            result = (Integer) value;
            scores[INT.index()] += 1;
        } else if (value instanceof String) {
            try {
                result = Integer.parseInt((String) value);
                scores[INT.index()] += 1;
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        return next.execute(result, context);
    }
}
