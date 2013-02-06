package com.treasure_data.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.type.MapValue;
import org.msgpack.type.ValueFactory;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.Config;
import com.treasure_data.commands.bulk_import.CSVPreparePartsRequest;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;
import com.treasure_data.file.MsgpackGZIPFileWriter;

public class TestMsgpackGZIPFileWriter {

    static class MockMsgpackGZIPFileWriter extends MsgpackGZIPFileWriter {

        private ByteArrayOutputStream out;

        public MockMsgpackGZIPFileWriter(PreparePartsRequest request,
                String infileName) throws CommandException {
            super(request, infileName);
        }

        @Override
        public void reopenOutputFile() throws CommandException {
            out = new ByteArrayOutputStream();
            dout = new DataSizeChecker(out);
            packer = msgpack.createPacker(dout);
        }

        @Override
        public void close() throws CommandException {
            if (dout != null) {
                try {
                    dout.close();
                } catch (IOException e) {
                    throw new CommandException(e);
                }
                dout = null;
            }
            packer = null;
        }

        public byte[] getByteArray() {
            return out.toByteArray();
        }
    }

    @Test
    public void writeSampleRow() throws Exception {
        Properties props = new Properties();
        props.setProperty(Config.BI_PREPARE_PARTS_FORMAT, "csv");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNS, "v0,v1,v2,v3,time");
        props.setProperty(Config.BI_PREPARE_PARTS_COLUMNTYPES,
                "string,int,long,double,long");
        props.setProperty(Config.BI_PREPARE_PARTS_OUTPUTDIR, "out");
        props.setProperty(Config.BI_PREPARE_PARTS_SPLIT_SIZE, "" + (16 * 1024));
        PreparePartsRequest req = new CSVPreparePartsRequest("csv",
                new String[0], props);

        MockMsgpackGZIPFileWriter w = new MockMsgpackGZIPFileWriter(req,
                "foo/bar.csv");
        w.writeBeginRow(5); // 1st row
        w.write("v0");
        w.write("c00");
        w.write("v1");
        w.write(0);
        w.write("v2");
        w.write(0L);
        w.write("v3");
        w.write(0.0);
        w.write("time");
        w.write(0L);
        w.writeEndRow();
        w.writeBeginRow(5); // 2nd row
        w.write("v0");
        w.write("c01");
        w.write("v1");
        w.write(1);
        w.write("v2");
        w.write(1L);
        w.write("v3");
        w.write(1.1);
        w.write("time");
        w.write(1L);
        w.writeEndRow();

        w.close();

        byte[] bytes = w.getByteArray();
        Unpacker unpacker = new MessagePack()
                .createUnpacker(new ByteArrayInputStream(bytes));
        UnpackerIterator iter = unpacker.iterator();

        assertTrue(iter.hasNext());
        {
            MapValue got = (MapValue) iter.next();
            assertEquals(5, got.size());
            assertEquals(ValueFactory.createRawValue("c00"),
                    got.get(ValueFactory.createRawValue("v0")));
            assertEquals(ValueFactory.createIntegerValue(0),
                    got.get(ValueFactory.createRawValue("v1")));
            assertEquals(ValueFactory.createIntegerValue(0L),
                    got.get(ValueFactory.createRawValue("v2")));
            assertEquals(ValueFactory.createFloatValue(0.0),
                    got.get(ValueFactory.createRawValue("v3")));
            assertEquals(ValueFactory.createIntegerValue(0L),
                    got.get(ValueFactory.createRawValue("time")));
        }
        assertTrue(iter.hasNext());
        {
            MapValue got = (MapValue) iter.next();
            assertEquals(5, got.size());
            assertEquals(ValueFactory.createRawValue("c01"),
                    got.get(ValueFactory.createRawValue("v0")));
            assertEquals(ValueFactory.createIntegerValue(1),
                    got.get(ValueFactory.createRawValue("v1")));
            assertEquals(ValueFactory.createIntegerValue(1L),
                    got.get(ValueFactory.createRawValue("v2")));
            assertEquals(ValueFactory.createFloatValue(1.1),
                    got.get(ValueFactory.createRawValue("v3")));
            assertEquals(ValueFactory.createIntegerValue(1L),
                    got.get(ValueFactory.createRawValue("time")));
        }
        assertTrue(!iter.hasNext());
    }

}
