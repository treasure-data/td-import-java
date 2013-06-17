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

public class DoubleColumnProc extends AbstractColumnProc {

    public DoubleColumnProc(int index, String columnName,
            com.treasure_data.bulk_import.writer.FileWriter writer) {
        super(index, columnName, writer);
    }

    @Override
    public Object executeValue(final Object value) throws PreparePartsException {
        Double v = null;

        if (value instanceof Double) {
            v = (Double) value;
        } else if (value instanceof String) {
            try {
                v = Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                throw new PreparePartsException(String.format(
                        "'%s' could not be parsed as an Double", value));
            }
        } else {
            final String actualClassName = value.getClass().getName();
            throw new PreparePartsException(String.format(
                    "the input value should be of type Double or String but is of type %s",
                    actualClassName));
        }

        writer.writeDouble(v);
        return v;
    }

}
