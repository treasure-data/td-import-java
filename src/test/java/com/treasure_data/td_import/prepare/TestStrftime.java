package com.treasure_data.td_import.prepare;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Test;

public class TestStrftime {
    private Strftime t;

    @Test
    public void testMillisecond() {
        t = new Strftime("%Y-%m-%d %H:%M:%S%z");
        assertEquals(1427342681L, t.getTime("2015-03-26 04:04:41+0000"));

        t = new Strftime("%Y-%m-%d %H:%M:%S.483647%z");
        assertEquals(1427342681L, t.getTime("2015-03-26 04:04:41.483647+0000"));

        t = new Strftime("%Y-%m-%d %H:%M:%S.483%z");
        assertEquals(1427342681L, t.getTime("2015-03-26 04:04:41.483+0000"));

        t = new Strftime("%Y-%m-%d %H:%M:%S.%L%z");
        assertEquals(1427342681L, t.getTime("2015-03-26 04:04:41.483+0000"));

        // Timezone free test
        Calendar c = Calendar.getInstance();
        c.set(2015, 2, 26, 4, 4, 41);
        c.setTimeZone(TimeZone.getDefault());
        t = new Strftime("%Y-%m-%d %H:%M:%S");
        assertEquals(c.getTimeInMillis() / 1000, t.getTime("2015-03-26 04:04:41.483647"));
    }
}