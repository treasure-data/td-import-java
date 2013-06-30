package com.treasure_data.bulk_import.integration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;

@Ignore
public class HeaderlessCSVFileGenerator extends FileGenerator {
    private static final String LF = "\n";
    private static final String COMMA = ",";

    protected Map<String, Integer> headerMap;

    protected Object[] row;

    public HeaderlessCSVFileGenerator(String fileName, String[] header)
            throws IOException {
        super(fileName, header);

        headerMap = new HashMap<String, Integer>(header.length);
        for (int i = 0; i < header.length; i++) {
            headerMap.put(header[i], i);
        }
        row = new Object[header.length];
    }

    public void writeHeader() throws IOException {
        // header-less
    }

    public void write(Map<String, Object> map) throws IOException {
        for (Map.Entry<String, Object> e : map.entrySet()) {
            int i = headerMap.get(e.getKey());
            row[i] = e.getValue();
        }

        for (int i = 0; i < header.length; i++) {
            out.write(row[i].toString().getBytes());
            if (i != header.length - 1) {
                out.write(COMMA.getBytes());
            }
        }
        out.write(LF.getBytes());
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
