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
package com.treasure_data.td_import.reader;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.logging.Logger;

import com.treasure_data.td_import.model.TimeColumnSampling;
import com.treasure_data.td_import.model.TimeColumnValue;
import com.treasure_data.td_import.prepare.ApachePrepareConfiguration;
import com.treasure_data.td_import.prepare.PreparePartsException;
import com.treasure_data.td_import.prepare.Strftime;
import com.treasure_data.td_import.prepare.Task;
import com.treasure_data.td_import.writer.FileWriter;

public class ApacheFileReader extends RegexFileReader<ApachePrepareConfiguration> {

    private static final Logger LOG = Logger.getLogger(ApacheFileReader.class
            .getName());

    // 127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
    public static final String commonLogPatString =
          "^([^ ]*) [^ ]* ([^ ]*) \\[([^\\]]*)\\] \"(\\S+)(?: +([^ ]*) +\\S*)?\" ([^ ]*) ([^ ]*)(?: \"([^\\\"]*)\" \"([^\\\"]*)\")?$";

    public ApacheFileReader(ApachePrepareConfiguration conf, FileWriter writer)
            throws PreparePartsException {
        super(conf, writer);
    }

    @Override
    public void validateRowSize(TimeColumnSampling[] sampleColumnValues,
            List<String> row, int lineNum) throws PreparePartsException {
        // ignore
    }

    @Override
    public void setTimeColumnValue(TimeColumnSampling[] sampleColumnValues,
            int timeColumnIndex, int aliasTimeColumnIndex) {
        timeColumnValue = new TimeColumnValue(2, new Strftime("%d/%b/%Y:%H:%M:%S %z"));
    }
}
