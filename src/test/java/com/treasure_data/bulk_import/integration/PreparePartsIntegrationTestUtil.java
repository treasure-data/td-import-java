package com.treasure_data.bulk_import.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.msgpack.MessagePack;
import org.msgpack.type.MapValue;
import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;
import org.msgpack.unpacker.UnpackerIterator;

@Ignore
public class PreparePartsIntegrationTestUtil {
    private static Value STRING_VALUE = ValueFactory.createRawValue("string-value");
    private static Value INT_VALUE = ValueFactory.createRawValue("int-value");
    private static Value DOUBLE_VALUE = ValueFactory.createRawValue("double-value");
    private static Value TIME = ValueFactory.createRawValue("time");

    static final String INPUT_DIR = "./src/test/resources/in/";
    static final String OUTPUT_DIR = "./src/test/resources/out/";

    protected Properties props;
    protected List<String> args;

    @Before
    public void createResources() throws Exception {
        props = new Properties();
        args = new ArrayList<String>();
    }

    @After
    public void destroyResources() throws Exception {
    }

    public void assertDataEquals(String srcFileName, String dstFileName) throws Exception {
        MessagePack msgpack = new MessagePack();

        InputStream srcIn = new BufferedInputStream(new GZIPInputStream(new FileInputStream(srcFileName)));
        InputStream dstIn = new BufferedInputStream(new GZIPInputStream(new FileInputStream(dstFileName)));

        UnpackerIterator srcIter = msgpack.createUnpacker(srcIn).iterator();
        UnpackerIterator dstIter = msgpack.createUnpacker(dstIn).iterator();

        while (srcIter.hasNext() && dstIter.hasNext()) {
            MapValue srcMap = srcIter.next().asMapValue();
            MapValue dstMap = dstIter.next().asMapValue();

            assertMapValueEquals(srcMap, dstMap);
        }

        assertFalse(srcIter.hasNext());
        assertFalse(dstIter.hasNext());
    }

    private void assertMapValueEquals(MapValue src, MapValue dst) {
        assertTrue(src.containsKey(STRING_VALUE));
        assertEquals(src.get(STRING_VALUE), dst.get(STRING_VALUE));

        assertTrue(src.containsKey(INT_VALUE));
        assertEquals(src.get(INT_VALUE), dst.get(INT_VALUE));

        assertTrue(src.containsKey(DOUBLE_VALUE));
        assertEquals(src.get(DOUBLE_VALUE), dst.get(DOUBLE_VALUE));

        assertTrue(src.containsKey(TIME));
        assertEquals(src.get(TIME), dst.get(TIME));
    }

}
