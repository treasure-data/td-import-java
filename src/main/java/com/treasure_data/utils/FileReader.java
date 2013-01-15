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

import java.io.File;

import org.msgpack.type.Value;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.bulk_import.PreparePartsRequest;

public abstract class FileReader {

    protected abstract void initReader(PreparePartsRequest request, File file)
            throws CommandException;

    public abstract Value[] readRecord() throws CommandException;

    public abstract void close() throws CommandException;
}
