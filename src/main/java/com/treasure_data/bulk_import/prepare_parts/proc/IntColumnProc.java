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

public class IntColumnProc extends AbstractColumnProc {

    public IntColumnProc(int index, String columnName,
            com.treasure_data.bulk_import.writer.FileWriter writer) {
        super(index, columnName, writer);
    }

    @Override
    public Object executeValue(final Object value) throws PreparePartsException {
        Integer v = null;
        if (value instanceof Integer) {
            v = (Integer) value;
        } else if (value instanceof String) {
            try {
                v = Integer.parseInt((String) value);
                writer.write(v);
            } catch (NumberFormatException e) {
                throw new PreparePartsException(String.format(
                        "'%s' could not be parsed as an Integer", value));
            }
        } else {
            final String actualClassName = value.getClass().getName();
            throw new PreparePartsException(String.format(
                    "the input value should be of type Integer or String but is of type %s",
                    actualClassName));
        }

        writer.write(v);
        return v;
    }

}
