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
    protected String[] columnNames;
    protected ColumnType[] columnTypes;

    public static class Context {

        public void createState(TestCSVFileReader reader) throws Exception {
            reader.fileName = "./file01.csv";
            reader.numLine = reader.rand.nextInt(10) + 1;
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

        public void assertStateEquals(TestCSVFileReader reader) {
            assertArrayEquals(reader.columnNames, reader.reader.getColumnNames());
            assertArrayEquals(reader.columnTypes, reader.reader.getColumnTypes());
            assertTrue(reader.reader.getTimeColumnValue() instanceof TimeColumnValue);
            assertTrue(reader.reader.getSkipColumns().isEmpty());
        }
    }

    protected TestCSVFileReader.Context state;

    @Before
    public void createResources() throws Exception {
        state = new TestCSVFileReader.Context();
        state.createState(this);
        super.createResources();
    }

    protected void createProperties() throws Exception {
        super.createProperties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE, "" + numLine);
    }

    @Override
    public void createFileReader() throws Exception {
        super.createFileReader();
        reader = conf.getFormat().createFileReader(conf, writer);
    }

    @Override
    public void createTask() throws Exception {
        task = new PrepareProcessor.Task(fileName);
        task = spy(task);
        task.isTest = true;
        task.testText = state.generateCSVText(this);
    }

    @After
    public void destroyResources() throws Exception {
        super.createResources();
    }

    @Test
    public void checkStateWhenReaderConfiguration() throws Exception {
        reader.configure(task);

        state.assertStateEquals(this);
    }
}
