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
package com.treasure_data.file;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.CSVPreparePartsRequest;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;
import com.treasure_data.commands.bulk_import.PreparePartsResult;

public class FileParserFactory {

    // TODO #MN should consider type parameters
    public static FileParser<?, ?> newInstance(PreparePartsRequest request,
            PreparePartsResult result) throws CommandException {
        PreparePartsRequest.Format format = request.getFormat();
        if (format.equals(PreparePartsRequest.Format.CSV)
                || format.equals(PreparePartsRequest.Format.TSV)) {
            return new CSVFileParser((CSVPreparePartsRequest) request, result);
        } else if (format.equals(PreparePartsRequest.Format.JSON)) {
            //return new JSONFileParser(request, result);
            throw new CommandException(new UnsupportedOperationException(
                    "format: " + format));
        } else if (format.equals(PreparePartsRequest.Format.MSGPACK)) {
            throw new CommandException(new UnsupportedOperationException(
                    "format: " + format));
        } else {
            throw new CommandException("Invalid format: " + format);
        }
    }
}
