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
package com.treasure_data.tools;

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.bulk_import.PreparePartsCommand;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;
import com.treasure_data.commands.bulk_import.PreparePartsResult;

public class BulkImportTool {
    private static final Logger LOG = Logger.getLogger(BulkImportTool.class
            .getName());

    /**
     * > td bulk_import:prepare_parts2
     * usage:
     *   $ java BulkImportTool prepare_parts <files...>
     * example:
     *   $ java BulkImportTool prepareParts logs/*.csv \
     *         -Dtd.bulk_import.prepare_parts.format=csv \
     *         -Dtd.bulk_import.prepare_parts.columns=time,uid,price,count \
     *         -Dtd.bulk_import.prepare_parts.columntypes=long,string,long,int \
     *         -Dtd.bulk_import.prepare_parts.time_column=time \
     *         -Dtd.bulk_import.prepare_parts.output_dir=./parts/
     * description:
     *   Convert files into part file format
     * options:
     *   -f, --format NAME                source file format [csv]
     *   -h, --columns NAME,NAME,...      column names (use --column-header instead if the first line has column names)
     *   -T, --column-types TYPE,TYPE,... column types
     *   -H, --column-header              first line includes column names
     *   -t, --time-column NAME           name of the time column
     *   -s, --split-size SIZE_IN_KB      size of each parts (default: 16384)
     *   -o, --output DIR                 output directory
     *
     * @param args
     * @param props
     * @throws Exception
     */
    public static void prepareParts(final String[] args, Properties props)
            throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("File names not specified");
        }

        LOG.info("Start prepare_parts command");

        String[] fileNames = new String[args.length - 1];
        for (int i = 0; i < args.length - 1; i++) {
            fileNames[i] = args[i + 1];
        }

        PreparePartsRequest request = new PreparePartsRequest(fileNames, props);
        PreparePartsResult result = new PreparePartsResult();

        PreparePartsCommand command = new PreparePartsCommand();
        command.execute(request, result);

        LOG.info("Finish prepare_parts command");
    }

    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Command not specified");
        }

        String commandName = args[0];
        Properties props = System.getProperties();
        if (commandName.equals("prepare_parts")) {
            prepareParts(args, props);
        } else {
            throw new IllegalArgumentException(
                    "Not support command: " + commandName);
        }
    }
}
