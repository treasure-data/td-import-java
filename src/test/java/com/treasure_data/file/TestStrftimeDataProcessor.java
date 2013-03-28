package com.treasure_data.file;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.treasure_data.file.proc.StrftimeDataProcessor;

public class TestStrftimeDataProcessor {

    @Test
    public void testSample() {
        StrftimeDataProcessor proc = new StrftimeDataProcessor("%Y-%m-%d %H:%M:%S"); // TODO
        Long t = (Long) proc.execute("2013-01-12 10:50:30", null);
        System.out.println(t);
    }

}
