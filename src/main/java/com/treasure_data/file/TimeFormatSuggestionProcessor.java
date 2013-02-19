package com.treasure_data.file;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.util.CsvContext;

import com.treasure_data.commands.CommandException;

public class TimeFormatSuggestionProcessor extends CellProcessorAdaptor {

    public static enum TimeFormat {
        INT("int", 0),
        LONG("long", 1),
        FLOAT("float", 2),
        RFC_822_1123_FORMAT("RFC_822_1123", 3),
        RFC_850_1036_FORMAT("RFC_850_1036", 4),
        APACHE_CLF_FORMAT("Apache_CLF", 5),
        ANSI_C_ASCTIME_FORMAT("ANSI_C_Asctime", 6);

        private String type;
        private int index;

        TimeFormat(String type, int index) {
            this.type = type;
            this.index = index;
        }

        public String type() {
            return type;
        }

        public int index() {
            return index;
        }

        public static TimeFormat fromString(String type) {
            return StringToTimeFormat.get(type);
        }

        public static TimeFormat fromInt(int index) {
            return IntToTimeFormat.get(index);
        }

        private static class StringToTimeFormat {
            private static final Map<String, TimeFormat> REVERSE_DICTIONARY;

            static {
                Map<String, TimeFormat> map = new HashMap<String, TimeFormat>();
                for (TimeFormat elem : TimeFormat.values()) {
                    map.put(elem.type, elem);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static TimeFormat get(String key) {
                return REVERSE_DICTIONARY.get(key);
            }
        }

        private static class IntToTimeFormat {
            private static final Map<Integer, TimeFormat> REVERSE_DICTIONARY;

            static {
                Map<Integer, TimeFormat> map = new HashMap<Integer, TimeFormat>();
                for (TimeFormat elem : TimeFormat.values()) {
                    map.put(elem.index, elem);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static TimeFormat get(Integer index) {
                return REVERSE_DICTIONARY.get(index);
            }
        }
    }

    private int[] scores = new int[] { 0, 0, 0, 0, 0, 0, 0 };
    private TimeFormatMatcher[] matchers;
    private int rowSize;

    TimeFormatSuggestionProcessor(int rowSize) {
        this.rowSize = rowSize;
        this.matchers = new TimeFormatMatcher[7];
        matchers[0] = new IntegerTimeFormatMatcher();
        matchers[1] = new LongTimeFormatMatcher();
        matchers[2] = new FloatTimeFormatMatcher();
        matchers[3] = new RFC_822_1123_FormatMatcher();
        matchers[4] = new RFC_850_1036_FormatMatcher();
        matchers[5] = new ApacheCLFFormatMatcher();
        matchers[6] = new ANSICAscTimeFormatMatcher();
    }

    void addHint() throws CommandException { // TODO e.g. strf time
        throw new UnsupportedOperationException();
    }

    TimeFormat getSuggestedTimeFormat() {
        int max = -rowSize;
        int maxIndex = 0;
        for (int i = 0; i < scores.length; i++) {
            if (max < scores[i]) {
                max = scores[i];
                maxIndex = i;
            }
        }
        return TimeFormat.fromInt(maxIndex);
    }

    TimeFormatProcessor createTimeFormatProcessor(TimeFormat tf)
            throws CommandException {
        switch (tf) {
        case INT:
            return new IntegerTimeFormatProcessor();
        case LONG:
            return new LongTimeFormatProcessor();
        case FLOAT:
            return new FloatTimeFormatProcessor();
        case  RFC_822_1123_FORMAT:
            return new RFC_822_1123_FormatProcessor();
        case RFC_850_1036_FORMAT:
            return new RFC_850_1036_FormatProcessor();
        case APACHE_CLF_FORMAT:
            return new ApacheCLFFormatProcessor();
        case ANSI_C_ASCTIME_FORMAT:
            return new ANSICAscTimeFormatProcessor();
        default:
            throw new CommandException("fatal error");
        }
    }

    @Override
    public Object execute(Object value, CsvContext context) {
        if (value == null) {
            // any score are not changed
            return null;
        }

        for (int i = 0; i < matchers.length; i++) {
            if (matchers[i].match(value)) {
                scores[i] += 1;
            }
        }

        // null object is returned TODO ??
        return next.execute(null, context);
    }

    public static interface TimeFormatMatcher {
        public boolean match(Object v);
    }

    public static class TimeFormatProcessor extends CellProcessorAdaptor {
        @Override
        public Object execute(Object value, CsvContext context) {
            return null;
        }
    }

    public static class IntegerTimeFormatMatcher implements TimeFormatMatcher {
        public boolean match(Object v) {
            if (v instanceof Integer) {
                return true;
            } else if (v instanceof String) {
                try {
                    Integer.parseInt((String) v);
                    return true;
                } catch (NumberFormatException e) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    public static class IntegerTimeFormatProcessor extends TimeFormatProcessor {
        @Override
        public Object execute(Object value, CsvContext context) {
            if (value instanceof Integer) {
                return (Integer) value;
            } else if (value instanceof String) {
                try {
                    return Integer.parseInt((String) value);
                } catch (NumberFormatException e) {
                    throw new SuperCsvCellProcessorException(String.format(
                            "'%s' could not be parsed as an Integer", value),
                            context, this, e);
                }
            } else {
                final String actualClassName = value.getClass().getName();
                throw new SuperCsvCellProcessorException(
                        String.format(
                                "the input value should be of type Integer or String but is of type %s",
                                actualClassName), context, this);
            }
        }
    }

    public static class LongTimeFormatMatcher implements TimeFormatMatcher {
        public boolean match(Object v) {
            if (v instanceof Long) {
                return true;
            } else if (v instanceof String) {
                try {
                    Long.parseLong((String) v);
                    return true;
                } catch (NumberFormatException e) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    public static class LongTimeFormatProcessor extends TimeFormatProcessor {
        @Override
        public Object execute(Object value, CsvContext context) {
            if (value instanceof Long) {
                return (Long) value;
            } else if (value instanceof Long) {
                try {
                    return Long.parseLong((String) value);
                } catch (NumberFormatException e) {
                    throw new SuperCsvCellProcessorException(String.format(
                            "'%s' could not be parsed as an Long", value),
                            context, this, e);
                }
            } else {
                final String actualClassName = value.getClass().getName();
                throw new SuperCsvCellProcessorException(
                        String.format(
                                "the input value should be of type Long or String but is of type %s",
                                actualClassName), context, this);
            }
        }
    }

    public static class FloatTimeFormatMatcher implements TimeFormatMatcher {
        public boolean match(Object v) {
            if (v instanceof Float) {
                return true;
            } else if (v instanceof String) {
                try {
                    Float.parseFloat((String) v);
                    return true;
                } catch (NumberFormatException e) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    public static class FloatTimeFormatProcessor extends TimeFormatProcessor {
        @Override
        public Object execute(Object value, CsvContext context) {
            if (value instanceof Float) {
                return (long) ((float) ((Float) value));
            } else if (value instanceof Float) {
                try {
                    return (long) Float.parseFloat((String) value);
                } catch (NumberFormatException e) {
                    throw new SuperCsvCellProcessorException(String.format(
                            "'%s' could not be parsed as an Float", value),
                            context, this, e);
                }
            } else {
                final String actualClassName = value.getClass().getName();
                throw new SuperCsvCellProcessorException(
                        String.format(
                                "the input value should be of type Float or String but is of type %s",
                                actualClassName), context, this);
            }
        }
    }

    public abstract static class SimpleDateFormatMatcher implements
            TimeFormatMatcher {
        public boolean match(Object v) {
            if (! (v instanceof String)) {
                return false;
            }

            String text = (String) v;
            ParsePosition pp = new ParsePosition(0);
            Date d = getFormat().parse(text, pp);
            return d != null && pp.getErrorIndex() == -1;
        }

        protected abstract SimpleDateFormat getFormat();
    }

    public abstract static class SimpleDateFormatProcessor extends TimeFormatProcessor {
        @Override
        public Object execute(Object value, CsvContext context) {
            if (value == null) {
                throw new SuperCsvCellProcessorException("value is null",
                        context, this);
            }

            if (!(value instanceof String)) {
                final String actualClassName = value.getClass().getName();
                throw new SuperCsvCellProcessorException(
                        String.format(
                                "the input value should be of type String but is of type %s",
                                actualClassName), context, this);
            }

            String text = (String) value;
            ParsePosition pp = new ParsePosition(0);
            Date d = getFormat().parse(text, pp);
            return d.getTime() / 1000;
        }

        protected abstract SimpleDateFormat getFormat();
    }

    public static class RFC_822_1123_FormatMatcher extends
            SimpleDateFormatMatcher {
        private final SimpleDateFormat RFC_822_1123_FORMAT = new SimpleDateFormat(
                "EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);

        @Override
        public SimpleDateFormat getFormat() {
            return RFC_822_1123_FORMAT;
        }
    }

    public static class RFC_822_1123_FormatProcessor extends
            SimpleDateFormatProcessor {
        private final SimpleDateFormat RFC_822_1123_FORMAT = new SimpleDateFormat(
                "EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);

        @Override
        public SimpleDateFormat getFormat() {
            return RFC_822_1123_FORMAT;
        }
    }

    public static class RFC_850_1036_FormatMatcher extends
            SimpleDateFormatMatcher {
        private final SimpleDateFormat RFC_850_1036_FORMAT = new SimpleDateFormat(
                "EEEE, dd-MMM-yy HH:mm:ss z", Locale.ENGLISH);

        @Override
        public SimpleDateFormat getFormat() {
            return RFC_850_1036_FORMAT;
        }
    }

    public static class RFC_850_1036_FormatProcessor extends
            SimpleDateFormatProcessor {
        private final SimpleDateFormat RFC_850_1036_FORMAT = new SimpleDateFormat(
                "EEEE, dd-MMM-yy HH:mm:ss z", Locale.ENGLISH);

        @Override
        public SimpleDateFormat getFormat() {
            return RFC_850_1036_FORMAT;
        }
    }

    public static class ApacheCLFFormatMatcher extends SimpleDateFormatMatcher {
        private final SimpleDateFormat APACHE_CLF_FORMAT = new SimpleDateFormat(
                "dd/MMM/yyyy HH:mm:ss Z", Locale.ENGLISH);

        @Override
        public SimpleDateFormat getFormat() {
            return APACHE_CLF_FORMAT;
        }
    }

    public static class ApacheCLFFormatProcessor extends
            SimpleDateFormatProcessor {
        private final SimpleDateFormat APACHE_CLF_FORMAT = new SimpleDateFormat(
                "dd/MMM/yyyy HH:mm:ss Z", Locale.ENGLISH);

        @Override
        public SimpleDateFormat getFormat() {
            return APACHE_CLF_FORMAT;
        }
    }

    public static class ANSICAscTimeFormatMatcher extends
            SimpleDateFormatMatcher {
        private final SimpleDateFormat ANSI_C_ASCTIME_FORMAT = new SimpleDateFormat(
                "EEE MMM d HH:mm:ss yyyy", Locale.ENGLISH);

        @Override
        public SimpleDateFormat getFormat() {
            return ANSI_C_ASCTIME_FORMAT;
        }
    }

    public static class ANSICAscTimeFormatProcessor extends
            SimpleDateFormatProcessor {
        private final SimpleDateFormat ANSI_C_ASCTIME_FORMAT = new SimpleDateFormat(
                "EEE MMM d HH:mm:ss yyyy", Locale.ENGLISH);

        @Override
        public SimpleDateFormat getFormat() {
            return ANSI_C_ASCTIME_FORMAT;
        }
    }
}
