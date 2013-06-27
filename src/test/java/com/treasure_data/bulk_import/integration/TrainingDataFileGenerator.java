package com.treasure_data.bulk_import.integration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.junit.Ignore;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

@Ignore
public class TrainingDataFileGenerator extends FileGenerator {

    protected Packer packer;

    protected List<Object> kvs;

    public TrainingDataFileGenerator(String fileName, String[] header) throws IOException {
        super(fileName, header);

        out = new GZIPOutputStream(this.out);
        packer = new MessagePack().createPacker(out);
        this.kvs = new ArrayList<Object>();
    }

    @Override
    public void writeHeader() throws IOException {
        // ignore
    }

    public void write(Map<String, Object> map) throws IOException {
        packer.write(map);
    }

    @Override
    public void close() throws IOException {
        if (packer != null) {
            packer.flush();
        }

        if (packer != null) {
            packer.close();
        }
    }
}
