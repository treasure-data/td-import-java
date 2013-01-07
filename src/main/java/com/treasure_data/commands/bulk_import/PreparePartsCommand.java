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
package com.treasure_data.commands.bulk_import;

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.Command;
import com.treasure_data.commands.CommandException;
import com.treasure_data.utils.FileConverter;
import com.treasure_data.utils.FileReader;
import com.treasure_data.utils.FileReaderFactory;
import com.treasure_data.utils.FileWriter;
import com.treasure_data.utils.FileWriterFactory;

public class PreparePartsCommand extends
        Command<PreparePartsRequest, PreparePartsResult> {
    private static final Logger LOG = Logger
            .getLogger(PreparePartsCommand.class.getName());

    @Override
    public void execute(PreparePartsRequest request, PreparePartsResult result)
            throws CommandException {
        LOG.fine(request.getName() + " command started");

        Properties props = request.getProperties();
        String fileName = request.getFileName();

        FileReader r = FileReaderFactory.newInstance(props, fileName);
        FileWriter w = FileWriterFactory.newInstance(props, fileName);
        FileConverter conv = new FileConverter(props);
        conv.convertFile(r, w);
        if (r != null) {
            try {
                r.close();
            } catch (CommandException e) {
                e.printStackTrace(); // TODO
            }
        }
        if (w != null) {
            try {
                w.close();
            } catch (CommandException e) {
                e.printStackTrace(); // TODO
            }
        }
        LOG.fine(request.getName() + " command finished");
    }

    public static void main(String[] args) throws Exception {
        Properties props = System.getProperties();
        props.setProperty("td.bulk_import.prepare_parts.columns", "time,name,price");
        props.setProperty("td.bulk_import.prepare_parts.columntypes", "long,string,long");
        props.setProperty("td.bulk_import.prepare_parts.time_column", "time");
        props.setProperty("td.bulk_import.prepare_parts.output_dir", "./out/");

        PreparePartsCommand command = new PreparePartsCommand();
        PreparePartsRequest request = new PreparePartsRequest(props);
        request.setFileName("./in/test.csv");
        PreparePartsResult result = new PreparePartsResult();

        command.execute(request, result);
    }
}
