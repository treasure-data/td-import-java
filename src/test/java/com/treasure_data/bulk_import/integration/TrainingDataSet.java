package com.treasure_data.bulk_import.integration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import org.junit.Ignore;

@Ignore
public class TrainingDataSet {

    private static final SimpleDateFormat format;
    private static final Object lock = new Object();

    static {
        format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    protected long numRows;
    protected long baseTime;
    protected Random rand = new Random(new Random().nextInt());

    public TrainingDataSet(long numRows, long baseTime) {
        this.numRows = numRows;
        this.baseTime = baseTime;
    }

    public void createDataFiles(FileGenerator[] gens) throws IOException {
        for (int i = 0; i < gens.length; i++) {
            gens[i].writeHeader();
        }

        Map<String, Object> row = new HashMap<String, Object>();
        for (long i = 0; i < numRows; i++) {
            for (int j = 0; j < gens.length; j++) {
                createRow(gens[j], (int) i, row);
                gens[j].write(row);
                row.clear();
            }
        }

        for (int i = 0; i < gens.length; i++) {
            gens[i].close();
        }
    }

    private void createRow(FileGenerator gen, int i, Map<String, Object> row) {
        for (int j = 0; j < gen.header.length; j++) {
            if (gen.header[j].startsWith("string-")) {
                row.put(gen.header[j], "muga" + rand.nextInt(100));
            } else if (gen.header[j].startsWith("int-")) {
                row.put(gen.header[j], rand.nextInt());
            } else if (gen.header[j].startsWith("double-")) {
                row.put(gen.header[j], rand.nextDouble());
            } else if (gen.header[j].startsWith("long-")) {
                row.put(gen.header[j], rand.nextInt());
            } else if (gen.header[j].equals("time")) {
                row.put(gen.header[j], baseTime + 60 * i);
            } else if (gen.header[j].equals("timestamp")) {
                row.put(gen.header[j], baseTime + 60 * i);
            } else if (gen.header[j].equals("timeformat")) {
                long t = (long)((baseTime + 60 * i) * 1000);
                String s = null;
                synchronized (lock) {
                    s = format.format(new Date(t));
                }
                row.put(gen.header[j], s);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

}
