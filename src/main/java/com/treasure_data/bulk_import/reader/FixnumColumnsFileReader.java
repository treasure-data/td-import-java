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
package com.treasure_data.bulk_import.reader;

import java.io.IOException;

import com.treasure_data.bulk_import.prepare.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare.PreparePartsException;
import com.treasure_data.bulk_import.prepare.Task;
import com.treasure_data.bulk_import.writer.FileWriter;

public abstract class FixnumColumnsFileReader<T extends PrepareConfiguration> extends FileReader<T> {
    public FixnumColumnsFileReader(T conf, FileWriter writer) throws PreparePartsException {
        super(conf, writer);
    }

    @Override
    public void configure(Task task) throws PreparePartsException {
        super.configure(task);
    }

    @Override
    public boolean readRow() throws IOException, PreparePartsException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void convertTypesOfColumns() throws PreparePartsException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

}