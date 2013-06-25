package com.treasure_data.bulk_import.reader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.spy;

import com.treasure_data.bulk_import.model.ColumnType;
import com.treasure_data.bulk_import.model.TimeColumnValue;
import com.treasure_data.bulk_import.prepare_parts.PrepareProcessor;
import com.treasure_data.commands.Config;

public class TestCSVFileReader extends FileReaderTestUtil {

    private static final String LF = "\n";
    private static final String COMMA = ",";

    protected String fileName;
    protected int numLine;

    protected AbstractContext context;

    public static class AbstractContext {
        public void createContext(TestCSVFileReader reader)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        public String generateCSVText(TestCSVFileReader reader) {
            throw new UnsupportedOperationException();
        }

        public void assertContextEquals(TestCSVFileReader reader) {
            assertArrayEquals(reader.columnNames, reader.reader.getColumnNames());
            assertArrayEquals(reader.columnTypes, reader.reader.getColumnTypes());
            assertTrue(reader.reader.getTimeColumnValue() instanceof TimeColumnValue);
            assertTrue(reader.reader.getSkipColumns().isEmpty()); // TODO
        }
    }

    public static class Context01 extends AbstractContext {
        public void createContext(TestCSVFileReader reader)
                throws Exception {
            reader.fileName = "./file01.csv";
            reader.columnNames = new String[] { "name", "count", "time" };
            reader.columnTypes = new ColumnType[] {
                    ColumnType.STRING, ColumnType.LONG, ColumnType.LONG };
        }

        public String generateCSVText(TestCSVFileReader reader) {
            StringBuilder sbuf = new StringBuilder();
            sbuf.append(reader.columnNames[0]).append(COMMA);
            sbuf.append(reader.columnNames[1]).append(COMMA);
            sbuf.append(reader.columnNames[2]).append(LF);
            for (int i = 0; i < reader.numLine; i++) {
                sbuf.append("muga" + i).append(COMMA);
                sbuf.append(i).append(COMMA);
                sbuf.append(reader.baseTime + 60 * i).append(LF);
            }
            return sbuf.toString();
        }
    }

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
    public void checkStateWhenReaderConfiguration() throws Exception {
        // create context
        context = new TestCSVFileReader.Context01();
        context.createContext(this);

        // create task
        task = new PrepareProcessor.Task(fileName);
        task = spy(task);
        task.isTest = true;
        task.testText = ((TestCSVFileReader.Context01)context).generateCSVText(this);

        // call configure(task)
        reader.configure(task);
        context.assertContextEquals(this);
    }
}
