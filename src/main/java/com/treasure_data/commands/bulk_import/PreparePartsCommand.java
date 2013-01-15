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

import java.io.File;
import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.Command;
import com.treasure_data.commands.CommandException;
import com.treasure_data.utils.FileParser;
import com.treasure_data.utils.FileParserFactory;
import com.treasure_data.utils.FileWriter;

public class PreparePartsCommand extends
        Command<PreparePartsRequest, PreparePartsResult> {
    private static final Logger LOG = Logger
            .getLogger(PreparePartsCommand.class.getName());

    @Override
    public void execute(PreparePartsRequest request, PreparePartsResult result)
            throws CommandException {
        LOG.info("Execute " + request.getName() + " command");

        Properties props = request.getProperties();
        File[] files = request.getFiles();

        for (File f : files) {
            execute(props, request, result, f);
        }

        LOG.info("Finish " + request.getName() + " command");
    }

    protected void execute(Properties props, PreparePartsRequest request,
            PreparePartsResult result, File file) throws CommandException {
        LOG.info("Read file: " + file.getName());

        FileParser r = FileParserFactory.newInstance(request, file);
        FileWriter w = new FileWriter(request, file);
        while (r.parseRow(w)) {
            ;
        }
        r.close();
        w.close();
    }
}
