package com.treasure_data.td_import.integration;

import java.io.IOException;
import java.util.Map;

import org.junit.Ignore;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

@Ignore
public class MessagePackFileGenerator extends FileGenerator {
    private Packer packer;

    public MessagePackFileGenerator(String fileName, String[] header)
            throws IOException {
        super(fileName, header);
        packer = new MessagePack().createPacker(out);
    }

    public void writeHeader() throws IOException {
        // doesn't have header
    }

    public void write(Map<String, Object> map) throws IOException {
        packer.write(map);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
