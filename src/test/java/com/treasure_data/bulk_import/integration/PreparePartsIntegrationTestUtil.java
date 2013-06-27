package com.treasure_data.bulk_import.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
import org.msgpack.unpacker.UnpackerIterator;

@Ignore
public class PreparePartsIntegrationTestUtil {

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
        for (Map.Entry<Value, Value> srcElm : src.entrySet()) {
            Value srcKey = srcElm.getKey();
            assertEquals(src.get(srcKey), dst.get(srcKey));
        }
    }
}
