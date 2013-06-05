//
// Treasure Data Bulk-Import Tool in Java
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
package com.treasure_data.bulk_import.prepare_parts.proc;

import java.util.ArrayList;
import java.util.List;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfig;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

public class ColumnProcessorGenerator {

    public ColumnProcessor[] generate(String[] columnNames, PrepareConfig.ColumnType[] columnTypes,
            com.treasure_data.bulk_import.prepare_parts.FileWriter writer)
            throws PreparePartsException {
        int len = columnTypes.length;
        List<ColumnProcessor> cprocs = new ArrayList<ColumnProcessor>(len);

        for (int i = 0; i < len; i++) {
            String cname = columnNames[i];
            ColumnProcessor cproc = new CSVColumnProcessor(cname, writer); // csv specific
        
            cprocs.add(cproc);
        }

        return cprocs.toArray(new ColumnProcessor[0]);
    }
}
