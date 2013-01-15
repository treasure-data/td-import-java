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
package com.treasure_data.commands.bulk_import.cellproc;

import java.util.ArrayList;
import java.util.List;

import com.treasure_data.commands.CommandException;

public class CellProcessorGen {
    public CellProcessorGen() {
    }

    public CellProcessor[] gen(String[] columnTypes) throws CommandException {
        int len = columnTypes.length;
        List<CellProcessor> cprocs = new ArrayList<CellProcessor>(len);
        for (int i = 0; i < len; i++) {
            CellProcessor cproc;
            String type = columnTypes[i];
            if (type.equals("string")) {
                cproc = new StringProcessor();
            } else if (type.equals("int")) {
                cproc = new IntProcessor();
            } else if (type.equals("long")) {
                cproc = new LongProcessor();
                // TODO any more...
            } else {
                throw new CommandException("Unsupported type: " + type);
            }
            cprocs.add(cproc);
        }
        return cprocs.toArray(new CellProcessor[0]);
    }
}
