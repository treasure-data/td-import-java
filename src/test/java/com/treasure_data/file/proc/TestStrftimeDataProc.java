package com.treasure_data.file.proc;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.treasure_data.file.proc.StrftimeDataProc;

public class TestStrftimeDataProc {

    @Test
    public void testSample() {
        StrftimeDataProc proc = new StrftimeDataProc("%Y-%m-%d %H:%M:%S"); // TODO
        Long t = (Long) proc.execute("2013-01-12 10:50:30", null);
        System.out.println(t);
    }

}
