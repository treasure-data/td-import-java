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

import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public class FileConverter {
    private static final Logger LOG = Logger.getLogger(FileConverter.class
            .getName());

    public FileConverter(PreparePartsRequest request) {
        initConverter(request);
    }

    public void initConverter(PreparePartsRequest request) {
    }

    public void convertFile(FileParser r, FileWriter w) throws CommandException {
        while (r.parseRow(w)) {
            ;
        }
    }

    public void close(FileParser r) throws CommandException {
        if (r != null) {
            r.close();
        }
    }

    public void close(FileWriter w) throws CommandException {
        if (w != null) {
            w.close();
        }
    }
}
