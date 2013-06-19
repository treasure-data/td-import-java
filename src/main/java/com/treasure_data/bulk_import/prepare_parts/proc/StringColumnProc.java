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

import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

public class StringColumnProc extends AbstractColumnProc {

    public StringColumnProc(int index, String columnName,
            com.treasure_data.bulk_import.writer.FileWriter writer) {
        super(index, columnName, writer);
    }

    @Override
    public Object executeValue(Object value) throws PreparePartsException {
        String v = null;

        if (!(value instanceof String)) {
            throw new PreparePartsException(String.format(
                    "'%s' cannot be cast to String type", value));
        }

        v = (String) value;
        writer.write(v);
        return v;
    }

}
