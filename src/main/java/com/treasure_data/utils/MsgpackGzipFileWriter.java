//
// Java Extension to CUI for Treasure Data
//
// Copyright (C) 2012 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.msgpack.MessagePack;
import org.msgpack.MessageTypeException;
import org.msgpack.packer.Packer;
import org.msgpack.template.Template;
import org.msgpack.template.Templates;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.commands.CommandException;

public class MsgpackGzipFileWriter extends FileWriter {
    private static final Logger LOG = Logger
            .getLogger(MsgpackGzipFileWriter.class.getName());

    static final Template<Map<String, Object>> tmpl = new ExtTemplate();

    static class ExtTemplate implements Template<Map<String, Object>> {
        @Override
        public void write(Packer pk, Map<String, Object> v) throws IOException {
            write(pk, v, false);
        }

        @Override
        public void write(Packer pk, Map<String, Object> v, boolean required)
                throws IOException {
            if (!(v instanceof Map)) {
                if (v == null) {
                    if (required) {
                        throw new MessageTypeException("Attempted to write null");
                    }
                    pk.writeNil();
                    return;
                }
                throw new MessageTypeException("Target is not a Map but " + v.getClass());
            }
            pk.writeMapBegin(v.size());
            for (Map.Entry<String, Object> pair : v.entrySet()) {
                Templates.TString.write(pk, pair.getKey());
                pk.write(pair.getValue()); // TODO refine??
            }
            pk.writeMapEnd();
        }

        @Override
        public Map<String, Object> read(Unpacker u, Map<String, Object> to)
                throws IOException {
            return read(u, to, false);
        }

        @Override
        public Map<String, Object> read(Unpacker u, Map<String, Object> to,
                boolean required) throws IOException {
            if (!required && u.trySkipNil()) {
                return null;
            }
            int n = u.readMapBegin();
            Map<String, Object> map;
            if (to != null) {
                map = (Map<String, Object>) to;
                map.clear();
            } else {
                map = new HashMap<String, Object>(n);
            }
            for (int i = 0; i < n; i++) {
                String key = Templates.TString.read(u, null);
                Object value;
                if (!key.equals("time")) {
                    value = Templates.TString.read(u, null);
                } else {
                    value = Templates.TLong.read(u, null);
                }
                map.put(key, value);
            }
            u.readMapEnd();
            return map;
        }
    }

    private MessagePack msgpack;
    private Packer packer;
    private GZIPOutputStream gzout;

    public MsgpackGzipFileWriter(Properties props, String fileName)
            throws CommandException {
        initWriter(props, fileName);
    }

    @Override
    public void initWriter(Properties props, String fileName)
            throws CommandException {
        msgpack = new MessagePack();
        try {
            File file = new File("./out/out.msgpack.gz"); // TODO
            BufferedOutputStream out = new BufferedOutputStream(
                    new FileOutputStream(file));
            gzout = new GZIPOutputStream(out);
            packer = msgpack.createPacker(gzout);
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    @Override
    public void writeRecord(Map<String, Object> record) throws CommandException {
        try {
            tmpl.write(packer, record);
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    @Override
    public void close() throws CommandException {
        if (gzout != null) {
            try {
                gzout.close();
            } catch (IOException e) {
                throw new CommandException(e);
            }
        }
    }
}
