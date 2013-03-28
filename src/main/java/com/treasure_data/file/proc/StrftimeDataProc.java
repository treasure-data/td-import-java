package com.treasure_data.file.proc;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.catalina.util.Strftime;
import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.cellprocessor.ift.StringCellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

public class StrftimeDataProc extends CellProcessorAdaptor implements
        StringCellProcessor {
    private static class ExtStrftime extends Strftime {
        public ExtStrftime(String origFormat) {
            super(origFormat);
        }

        public SimpleDateFormat getSimpleDateFormat() {
            return simpleDateFormat;
        }
    }

    private static SimpleDateFormat simpleFormat;

    public StrftimeDataProc(String dateFormat) {
        super();
        if (dateFormat == null) {
            throw new NullPointerException("dateFormat should not be null");
        }
        simpleFormat = new ExtStrftime(dateFormat).getSimpleDateFormat();
    }

    /**
     * {@inheritDoc}
     *
     * @throws SuperCsvCellProcessorException
     *             if value is null, isn't a String, or can't be parsed to a
     *             Date
     */
    public Object execute(final Object value, final CsvContext context) {
        validateInputNotNull(value, context);

        if (!(value instanceof String)) {
            final String actualClassName = value.getClass().getName();
            throw new SuperCsvCellProcessorException(String.format(
                    "the input value shoud be of type String but is of type %s",
                    actualClassName), context, this);
        }

        String v = (String) value;
        try {
            Long result = simpleFormat.parse(v).getTime() / 1000;
            return next.execute(result, context);
        } catch (final ParseException e) {
            throw new SuperCsvCellProcessorException(String.format(
                    "'%s' could not be parsed as a Date", value), context,
                    this, e);
        }
    }
}