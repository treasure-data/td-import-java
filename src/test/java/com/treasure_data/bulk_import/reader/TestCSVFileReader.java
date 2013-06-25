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

    @Before
    public void createResources() throws Exception {
        createFileContent();
        super.createResources();
    }

    public void createFileContent() throws Exception {
        fileName = "./file01.csv";
        numLine = rand.nextInt(10) + 1;
        columnNames = new String[] { "name", "count", "time" };
        columnTypes = new ColumnType[] { ColumnType.STRING, ColumnType.LONG, ColumnType.LONG };
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
        task.testText = generateCSVText();
    }

    private String generateCSVText() {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(columnNames[0]).append(COMMA);
        sbuf.append(columnNames[1]).append(COMMA);
        sbuf.append(columnNames[2]).append(LF);
        for (int i = 0; i < numLine; i++) {
            sbuf.append("muga" + i).append(COMMA);
            sbuf.append(i).append(COMMA);
            sbuf.append(baseTime + 60 * i).append(LF);
        }
        return sbuf.toString();
    }

    @After
    public void destroyResources() throws Exception {
        super.createResources();
    }

    @Test
    public void checkStateWhenReaderConfiguration() throws Exception {
        reader.configure(task);

        assertArrayEquals(columnNames, reader.getColumnNames());
        assertArrayEquals(columnTypes, reader.getColumnTypes());
        assertTrue(reader.getTimeColumnValue() instanceof TimeColumnValue);
        assertTrue(reader.getSkipColumns().isEmpty());
    }
}
