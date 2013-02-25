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
    private int[] scores = new int[] { 0, 0, 0, 0 };
    protected int rowSize;
    protected int hintScore;

    TypeSuggestionProcessor(int rowSize, int hintScore) {
        this.rowSize = rowSize;
        this.hintScore = hintScore;
    }

    void addHint(String typeHint) throws CommandException {
        if (typeHint == null) {
            throw new NullPointerException("type hint is null.");
        }

        CSVPreparePartsRequest.ColumnType type = ColumnType.fromString(typeHint);
        if (type == null) { // fatal error
            throw new CommandException("unsupported type: " + typeHint);
        }

        switch (type) {
        case INT:
            scores[INT.index()] += hintScore;
            break;
        case LONG:
            scores[LONG.index()] += hintScore;
            break;
        case DOUBLE:
            scores[DOUBLE.index()] += hintScore;
            break;
        case STRING:
            scores[STRING.index()] += hintScore;
            break;
        default:
            throw new CommandException("fatal error");
        }
    }

    ColumnType getSuggestedType() {
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

    void printScores() {
        for (int i = 0; i < scores.length; i++) {
            System.out.println(scores[i]);
        }
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
