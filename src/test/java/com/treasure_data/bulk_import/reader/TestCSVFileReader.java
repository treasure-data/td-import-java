package com.treasure_data.bulk_import.reader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.spy;

import com.treasure_data.bulk_import.model.AliasTimeColumnValue;
import com.treasure_data.bulk_import.model.ColumnType;
import com.treasure_data.bulk_import.model.TimeColumnValue;
import com.treasure_data.bulk_import.model.TimeValueTimeColumnValue;
import com.treasure_data.bulk_import.prepare_parts.PrepareProcessor;
import com.treasure_data.commands.Config;

public class TestCSVFileReader extends FileReaderTestUtil {

    private static final String LF = "\n";
    private static final String COMMA = ",";

    public static interface Context {
        public void createContext(TestCSVFileReader reader) throws Exception;

        public String generateCSVText(TestCSVFileReader reader);

        public void assertContextEquals(TestCSVFileReader reader);
    }

    public static class Context01 implements Context {
        public void createContext(TestCSVFileReader test)
                throws Exception {
            test.columnNames = new String[] { "name", "count", "time" };
            test.columnTypes = new ColumnType[] {
                    ColumnType.STRING, ColumnType.LONG, ColumnType.LONG };
        }

        public String generateCSVText(TestCSVFileReader test) {
            StringBuilder sbuf = new StringBuilder();
            sbuf.append(test.columnNames[0]).append(COMMA);
            sbuf.append(test.columnNames[1]).append(COMMA);
            sbuf.append(test.columnNames[2]).append(LF);
            for (int i = 0; i < test.numLine; i++) {
                sbuf.append("muga" + i).append(COMMA);
                sbuf.append(i).append(COMMA);
                sbuf.append(test.baseTime + 60 * i).append(LF);
            }
            return sbuf.toString();
        }

        public void assertContextEquals(TestCSVFileReader test) {
            assertArrayEquals(test.columnNames, test.reader.getColumnNames());
            assertArrayEquals(test.columnTypes, test.reader.getColumnTypes());
            assertTrue(test.reader.getTimeColumnValue() instanceof TimeColumnValue);
            assertTrue(test.reader.getSkipColumns().isEmpty());
        }
    }

    public static class Context02 implements Context {
        public long getTimeValue() {
            return 12345;
        }

        public void createContext(TestCSVFileReader test)
                throws Exception {
            test.columnNames = new String[] { "name", "count" };
            test.columnTypes = new ColumnType[] {
                    ColumnType.STRING, ColumnType.LONG };
        }

        public String generateCSVText(TestCSVFileReader test) {
            StringBuilder sbuf = new StringBuilder();
            sbuf.append(test.columnNames[0]).append(COMMA);
            sbuf.append(test.columnNames[1]).append(LF);
            for (int i = 0; i < test.numLine; i++) {
                sbuf.append("muga" + i).append(COMMA);
                sbuf.append(i).append(LF);
            }
            return sbuf.toString();
        }

        public void assertContextEquals(TestCSVFileReader test) {
            assertArrayEquals(test.columnNames, test.reader.getColumnNames());
            assertArrayEquals(test.columnTypes, test.reader.getColumnTypes());
            assertTrue(test.reader.getTimeColumnValue() instanceof TimeValueTimeColumnValue);
            assertEquals(getTimeValue(), ((TimeValueTimeColumnValue)test.reader.getTimeColumnValue()).getTimeValue());
            assertTrue(test.reader.getSkipColumns().isEmpty());
        }
    }

    public static class Context03 implements Context {
        public String getAliasTimeColumn() {
            return "date_code";
        }

        public void createContext(TestCSVFileReader test)
                throws Exception {
            test.columnNames = new String[] { "date_code", "name", "count" };
            test.columnTypes = new ColumnType[] {
                    ColumnType.LONG, ColumnType.STRING, ColumnType.LONG };
        }

        public String generateCSVText(TestCSVFileReader test) {
            StringBuilder sbuf = new StringBuilder();
            sbuf.append(test.columnNames[0]).append(COMMA);
            sbuf.append(test.columnNames[1]).append(COMMA);
            sbuf.append(test.columnNames[2]).append(LF);
            for (int i = 0; i < test.numLine; i++) {
                sbuf.append(test.baseTime + 60 * i).append(COMMA);
                sbuf.append("muga" + i).append(COMMA);
                sbuf.append(i).append(LF);
            }
            return sbuf.toString();
        }

        public void assertContextEquals(TestCSVFileReader test) {
            assertArrayEquals(test.columnNames, test.reader.getColumnNames());
            assertArrayEquals(test.columnTypes, test.reader.getColumnTypes());
            assertTrue(test.reader.getTimeColumnValue() instanceof AliasTimeColumnValue);
            assertTrue(test.reader.getSkipColumns().isEmpty());
        }
    }

    protected String fileName = "./file.csv";
    protected int numLine;

    @Before
    public void createResources() throws Exception {
        super.createResources();
    }

    @Override
    public void createProperties() throws Exception {
        super.createProperties();

        numLine = rand.nextInt(10) + 1;

        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE, "" + numLine);

    }

    @Override
    public void createFileReader() throws Exception {
        super.createFileReader();
        reader = conf.getFormat().createFileReader(conf, writer);
    }

    @After
    public void destroyResources() throws Exception {
        super.createResources();
    }

    @Test
    public void checkStateWhenReaderConfiguration01() throws Exception {
        Context01 context = new Context01();

        // create context
        context.createContext(this);

        // create task
        task = new PrepareProcessor.Task(fileName);
        task = spy(task);
        task.isTest = true;
        task.testText = context.generateCSVText(this);

        // call configure(task)
        reader.configure(task);
        context.assertContextEquals(this);
    }

    @Test
    public void checkStateWhenReaderConfigurationWithTimeValue() throws Exception {
        Context02 context = new Context02();

        // override system properties:-(
        props.setProperty(Config.BI_PREPARE_PARTS_TIMEVALUE, "" + context.getTimeValue());
        createPrepareConfiguration();
        createFileWriter();
        createFileReader();

        // create context
        context.createContext(this);

        // create task
        task = new PrepareProcessor.Task(fileName);
        task = spy(task);
        task.isTest = true;
        task.testText = context.generateCSVText(this);

        // call configure(task)
        reader.configure(task);
        context.assertContextEquals(this);
    }

    @Test
    public void checkStateWhenReaderConfigurationWithAliasTimeColumn() throws Exception {
        Context03 context = new Context03();

        // override system properties:-(
        props.setProperty(Config.BI_PREPARE_PARTS_TIMECOLUMN, "" + context.getAliasTimeColumn());
        createPrepareConfiguration();
        createFileWriter();
        createFileReader();

        // create context
        context.createContext(this);

        // create task
        task = new PrepareProcessor.Task(fileName);
        task = spy(task);
        task.isTest = true;
        task.testText = context.generateCSVText(this);

        // call configure(task)
        reader.configure(task);
        context.assertContextEquals(this);
    }
}
