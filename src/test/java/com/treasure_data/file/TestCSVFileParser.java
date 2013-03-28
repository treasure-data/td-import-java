package com.treasure_data.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.Config;
import com.treasure_data.commands.bulk_import.CSVPreparePartsRequest;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;
import com.treasure_data.commands.bulk_import.PreparePartsResult;
import com.treasure_data.file.CSVFileParser;
import com.treasure_data.file.proc.TypeSuggestProc;

public class TestCSVFileParser {

    private CSVPreparePartsRequest request;
    private PreparePartsResult result;
    private CSVFileParser parser;
    private FileWriterTestUtil writer;

    @Before
    public void createResources() throws Exception {
        request = new CSVPreparePartsRequest();
        request.setSampleRowSize(Integer.parseInt(Config.BI_PREPARE_PARTS_SAMPLE_ROWSIZE_DEFAULTVALUE));
        request.setDelimiterChar(Config.BI_PREPARE_PARTS_DELIMITER_CSV_DEFAULTVALUE.charAt(0)); // ','
        request.setNewLine(CSVPreparePartsRequest.NewLine.LF); // '\n'
        request.setHasColumnHeader(true);
        request.setColumnNames(new String[0]);
        request.setAliasTimeColumn(null);
        request.setOnlyColumns(new String[0]);
        request.setExcludeColumns(new String[0]);
        request.setColumnTypes(new String[0]);
        result = new PreparePartsResult();

        parser = new CSVFileParser(request, result);

        writer = new FileWriterTestUtil(request, result);
    }

    @After
    public void deleteResources() throws Exception {
        parser.close();
        parser = null;

        writer.close();
        writer = null;
    }

    @Test
    public void checkTypeSuggest() throws Exception {
        {
            String[] values = new String[] {
                    "v0\n", "v1\n", "v2\n", "v3\n", "v4\n",
            };
            StringBuilder sbuf = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                sbuf.append(values[i]);
            }

            String text = sbuf.toString();
            byte[] bytes = text.getBytes();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            CsvPreference pref = new CsvPreference.Builder('"', ',', "\n").build();
            CsvListReader sampleReader = new CsvListReader(
                    new InputStreamReader(in), pref);

            TypeSuggestProc TSP = new TypeSuggestProc(values.length);
            CellProcessor[] procs = new CellProcessor[] { TSP };

            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.close();

            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.INT), 0);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.LONG), 0);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), 0);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);

            assertEquals(CSVPreparePartsRequest.ColumnType.STRING, TSP.getSuggestedType());
        }
        {
            String[] values = new String[] { "v0\n", "v1\n", "v2\n", "v3\n", "v4\n", };
            StringBuilder sbuf = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                sbuf.append(values[i]);
            }

            String text = sbuf.toString();
            byte[] bytes = text.getBytes();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            CsvPreference pref = new CsvPreference.Builder('"', ',', "\n")
                    .build();
            CsvListReader sampleReader = new CsvListReader(
                    new InputStreamReader(in), pref);

            TypeSuggestProc TSP = new TypeSuggestProc(values.length);
            CellProcessor[] procs = new CellProcessor[] { TSP };

            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.close();

            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.INT), 0);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.LONG), 0);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), 0);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);

            assertEquals(CSVPreparePartsRequest.ColumnType.STRING, TSP.getSuggestedType());
        }
        {
            String[] values = new String[] { "v0\n", "v1\n", "v2\n", "v3\n", "v4\n", };
            StringBuilder sbuf = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                sbuf.append(values[i]);
            }

            String text = sbuf.toString();
            byte[] bytes = text.getBytes();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            CsvPreference pref = new CsvPreference.Builder('"', ',', "\n")
                    .build();
            CsvListReader sampleReader = new CsvListReader(
                    new InputStreamReader(in), pref);

            TypeSuggestProc TSP = new TypeSuggestProc(values.length);
            CellProcessor[] procs = new CellProcessor[] { TSP };

            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.close();

            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.INT), 0);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.LONG), 0);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), 0);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);

            assertEquals(CSVPreparePartsRequest.ColumnType.STRING, TSP.getSuggestedType());
        }
        {
            String[] values = new String[] { "0\n", "1\n", "2\n", "3\n", "4\n", };
            StringBuilder sbuf = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                sbuf.append(values[i]);
            }

            String text = sbuf.toString();
            byte[] bytes = text.getBytes();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            CsvPreference pref = new CsvPreference.Builder('"', ',', "\n")
                    .build();
            CsvListReader sampleReader = new CsvListReader(
                    new InputStreamReader(in), pref);

            TypeSuggestProc TSP = new TypeSuggestProc(values.length);
            CellProcessor[] procs = new CellProcessor[] { TSP };

            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.read(procs);
            sampleReader.close();

            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.INT), values.length);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.LONG), values.length);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), values.length);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);

            assertEquals(CSVPreparePartsRequest.ColumnType.INT, TSP.getSuggestedType());
        }
        {
            String[] values = new String[] { "0\n", "1\n", "2\n", "3\n", "4\n", };
            StringBuilder sbuf = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                sbuf.append(values[i]);
            }

            String text = sbuf.toString();
            byte[] bytes = text.getBytes();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            CsvPreference pref = new CsvPreference.Builder('"', ',', "\n")
                    .build();
            CsvListReader sampleReader = new CsvListReader(
                    new InputStreamReader(in), pref);

            TypeSuggestProc TSP = new TypeSuggestProc(values.length);
            CellProcessor[] procs = new CellProcessor[] { TSP };

            for (int i = 0; i < values.length; i++) {
                sampleReader.read(procs);
            }
            sampleReader.close();

            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.INT), values.length);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.LONG), values.length);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.DOUBLE), values.length);
            assertEquals(TSP.getScore(CSVPreparePartsRequest.ColumnType.STRING), values.length);

            assertEquals(CSVPreparePartsRequest.ColumnType.INT, TSP.getSuggestedType());
        }
    }

    @Test
    public void parseIntColumn() throws Exception {
        // parser setting
        String text =
                "v0,time\n" +
                "0,12345\n" +
                "1,12345\n" +
                "2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", 0, "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", 1, "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", 2, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseIntColumnThatNullValueIncluded() throws Exception {
        // parser setting
        String text =
                "v0,time\n" +
                ",12345\n" +
                "1,12345\n" +
                ",12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", null, "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", 1, "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", null, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseDoubleColumn() throws Exception {
        // parser setting
        String text =
                "v0,time\n" +
                "0.0,12345\n" +
                "1.1,12345\n" +
                "2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", 0.0, "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", 1.1, "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", 2.2, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseDoubleColumnThatNullValueIncluded() throws Exception {
        // parser setting
        String text =
                "v0,time\n" +
                ",12345\n" +
                "1.1,12345\n" +
                ",12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", null, "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", 1.1, "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", null, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseStringColumn() throws Exception {
        // parser setting
        String text =
                "v0,time\n" +
                "c00,12345\n" +
                "c01,12345\n" +
                "c02,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", "c00", "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", "c01", "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", "c02", "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseStringColumnThatNullValueIncluded() throws Exception {
        // parser setting
        String text =
                "v0,time\n" +
                ",12345\n" +
                "c01,12345\n" +
                ",12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", null, "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", "c01", "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", null, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseIntDoubleStringColumns() throws Exception {
        // parser setting
        String text =
                "v0,v1,v2,v3,time\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c00", "v1", 0, "v2", 0, "v3", 0.0, "time", 12345 });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c10", "v1", 1, "v2", 1, "v3", 1.1, "time", 12345 });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c20", "v1", 2, "v2", 2, "v3", 2.2, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseColumnsThatIncludeNullValue() throws Exception {
        // parser setting
        String text =
                "v0,v1,v2,v3,time\n" +
                "c00,0,0,0.0,12345\n" +
                ",,,,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c00", "v1", 0, "v2", 0, "v3", 0.0, "time", 12345 });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", null, "v1", null, "v2", null, "v3", null, "time", 12345 });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c20", "v1", 2, "v2", 2, "v3", 2.2, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseSeveralTypeColumn() throws Exception {
        // parser setting
        String text =
                "v0,time\n" +
                "0,12345\n" +
                "c10,12345\n" +
                ",12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", "0", "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", "c10", "time", 12345 });
        writer.setColSize(2);
        writer.setRow(new Object[] { "v0", null, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndAliasColumnName01() throws Exception {
        // request setting
        request.setAliasTimeColumn("timestamp");

        // parser setting
        String text =
                "v0,v1,v2,v3,timestamp\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(6);
        writer.setRow(new Object[] { "v0", "c00", "v1", 0, "v2", 0, "v3", 0.0, "timestamp", 12345, "time", 12345L });
        writer.setColSize(6);
        writer.setRow(new Object[] { "v0", "c10", "v1", 1, "v2", 1, "v3", 1.1, "timestamp", 12345, "time", 12345L });
        writer.setColSize(6);
        writer.setRow(new Object[] { "v0", "c20", "v1", 2, "v2", 2, "v3", 2.2, "timestamp", 12345, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndAliasColumnName02() throws Exception {
        // request setting
        request.setAliasTimeColumn("timestamp");

        // parser setting
        String text =
                "timestamp,v0,v1,v2,v3\n" +
                "12345,c00,0,0,0.0\n" +
                "12345,c10,1,1,1.1\n" +
                "12345,c20,2,2,2.2\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(6);
        writer.setRow(new Object[] { "timestamp", 12345, "v0", "c00", "v1", 0, "v2", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(6);
        writer.setRow(new Object[] { "timestamp", 12345, "v0", "c10", "v1", 1, "v2", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(6);
        writer.setRow(new Object[] { "timestamp", 12345, "v0", "c20", "v1", 2, "v2", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndTimeValue() throws Exception {
        // request setting
        request.setTimeValue(12345L);

        // parser setting
        String text =
                "v0,v1,v2,v3\n" +
                "c00,0,0,0.0\n" +
                "c10,1,1,1.1\n" +
                "c20,2,2,2.2\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c00", "v1", 0, "v2", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c10", "v1", 1, "v2", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c20", "v1", 2, "v2", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseOnlyColumns() throws Exception {
        // request setting
        request.setOnlyColumns(new String[] { "v1", "v3", "time" });

        // parser setting
        String text =
                "v0,v1,v2,v3,time\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345 });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345 });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndAliasTimeColumnAsOnlyColumns01()
            throws Exception {
        // request setting
        request.setAliasTimeColumn("timestamp");
        request.setOnlyColumns(new String[] { "v1", "v3", "time" });

        // parser setting
        String text =
                "v0,v1,v2,v3,timestamp\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndAliasTimeColumnAsOnlyColumns02()
            throws Exception {
        // request setting
        request.setAliasTimeColumn("timestamp");
        request.setOnlyColumns(new String[] { "v1", "v3" });

        // parser setting
        String text =
                "v0,v1,v2,v3,timestamp\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndTimeValueAsOnlyColumns01() throws Exception {
        // request setting
        request.setTimeValue(12345L);
        request.setOnlyColumns(new String[] { "v1", "v3", "time" });

        // parser setting
        String text =
                "v0,v1,v2,v3\n" +
                "c00,0,0,0.0\n" +
                "c10,1,1,1.1\n" +
                "c20,2,2,2.2\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndTimeValueAsOnlyColumns02() throws Exception {
        // request setting
        request.setTimeValue(12345L);
        request.setOnlyColumns(new String[] { "v1", "v3" });

        // parser setting
        String text =
                "v0,v1,v2,v3\n" +
                "c00,0,0,0.0\n" +
                "c10,1,1,1.1\n" +
                "c20,2,2,2.2\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void throwCmdErrorWhenParseNoTimeColumnSpecifiedAsOnlyColumns()
            throws Exception {
        // request setting
        request.setOnlyColumns(new String[] { "v1", "v3" });

        // parser setting
        String text =
                "v0,v1,v2,v3,time\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        try {
            parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof CommandException);
        }
    }

    @Test
    public void parseExcludeColumns() throws Exception {
        // request setting
        request.setExcludeColumns(new String[] { "v0", "v2" });

        // parser setting
        String text =
                "v0,v1,v2,v3,time\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345 });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345 });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseTimeColumnSpecifiedAsExcludeColumns() throws Exception {
        // request setting
        request.setExcludeColumns(new String[] { "v0", "v2", "time" });

        // parser setting
        String text =
                "v0,v1,v2,v3,time\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345 });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345 });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndAliasTimeColumnAsExcludeColumns01()
            throws Exception {
        // request setting
        request.setAliasTimeColumn("timestamp");
        request.setExcludeColumns(new String[] { "v0", "v2", "timestamp" });

        // parser setting
        String text =
                "v0,v1,v2,v3,timestamp\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    public void parseNoTimeColumnAndAliasTimeColumnAsExcludeColumns02()
            throws Exception {
        // request setting
        request.setAliasTimeColumn("timestamp");
        // even though 'time' is specified as exclude-columns,
        // 'time' is added to the result
        request.setExcludeColumns(new String[] { "v0", "v2", "timestamp", "time" });

        // parser setting
        String text =
                "v0,v1,v2,v3,timestamp\n" +
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndAliasTimeValueAsExcludeColumns01()
            throws Exception {
        // request setting
        request.setTimeValue(12345L);
        request.setExcludeColumns(new String[] { "v0", "v2" });

        // parser setting
        String text =
                "v0,v1,v2,v3\n" +
                "c00,0,0,0.0\n" +
                "c10,1,1,1.1\n" +
                "c20,2,2,2.2\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseNoTimeColumnAndAliasTimeValueAsExcludeColumns02()
            throws Exception {
        // request setting
        request.setTimeValue(12345L);
        request.setExcludeColumns(new String[] { "v0", "v2", "time" });

        // parser setting
        String text =
                "v0,v1,v2,v3\n" +
                "c00,0,0,0.0\n" +
                "c10,1,1,1.1\n" +
                "c20,2,2,2.2\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(3);
        writer.setRow(new Object[] { "v1", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseHeaderlessCSVText() throws Exception {
        // request setting
        request.setHasColumnHeader(false);
        request.setColumnNames(new String[] { "v0", "v1", "v2", "v3", "time" });

        // parser setting
        String text =
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c00", "v1", 0, "v2", 0, "v3", 0.0, "time", 12345 });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c10", "v1", 1, "v2", 1, "v3", 1.1, "time", 12345 });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c20", "v1", 2, "v2", 2, "v3", 2.2, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseHeaderlessTSVText() throws Exception {
        // request setting
        request.setFormat(PreparePartsRequest.Format.TSV);
        request.setDelimiterChar(Config.BI_PREPARE_PARTS_DELIMITER_TSV_DEFAULTVALUE.charAt(0)); // ','
        request.setHasColumnHeader(false);
        request.setColumnNames(new String[] { "v0", "v1", "v2", "v3", "time" });

        // parser setting
        String text =
                "c00\t0\t0\t0.0\t12345\n" +
                "c10\t1\t1\t1.1\t12345\n" +
                "c20\t2\t2\t2.2\t12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c00", "v1", 0, "v2", 0, "v3", 0.0, "time", 12345 });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c10", "v1", 1, "v2", 1, "v3", 1.1, "time", 12345 });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c20", "v1", 2, "v2", 2, "v3", 2.2, "time", 12345 });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseHeaderlessNoTimeColumnAndAliasColumn() throws Exception {
        // request setting
        request.setHasColumnHeader(false);
        request.setColumnNames(new String[] { "v0", "v1", "v2", "v3", "timestamp" });
        request.setAliasTimeColumn("timestamp");

        // parser setting
        String text =
                "c00,0,0,0.0,12345\n" +
                "c10,1,1,1.1,12345\n" +
                "c20,2,2,2.2,12345\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(6);
        writer.setRow(new Object[] { "v0", "c00", "v1", 0, "v2", 0, "v3", 0.0, "timestamp", 12345, "time", 12345L });
        writer.setColSize(6);
        writer.setRow(new Object[] { "v0", "c10", "v1", 1, "v2", 1, "v3", 1.1, "timestamp", 12345, "time", 12345L });
        writer.setColSize(6);
        writer.setRow(new Object[] { "v0", "c20", "v1", 2, "v2", 2, "v3", 2.2, "timestamp", 12345, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

    @Test
    public void parseHeaderlessNoTimeColumnAndTimeValue() throws Exception {
        // request setting
        request.setHasColumnHeader(false);
        request.setColumnNames(new String[] { "v0", "v1", "v2", "v3" });
        request.setTimeValue(12345L);

        // parser setting
        String text =
                "c00,0,0,0.0\n" +
                "c10,1,1,1.1\n" +
                "c20,2,2,2.2\n";
        byte[] bytes = text.getBytes();
        parser.initParser(FileParser.UTF_8, new ByteArrayInputStream(bytes));
        parser.startParsing(FileParser.UTF_8, new ByteArrayInputStream(bytes));

        // writer
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c00", "v1", 0, "v2", 0, "v3", 0.0, "time", 12345L });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c10", "v1", 1, "v2", 1, "v3", 1.1, "time", 12345L });
        writer.setColSize(5);
        writer.setRow(new Object[] { "v0", "c20", "v1", 2, "v2", 2, "v3", 2.2, "time", 12345L });

        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertTrue(parser.parseRow(writer));
        assertFalse(parser.parseRow(writer));

        assertEquals(3, parser.getRowNum());
    }

}
