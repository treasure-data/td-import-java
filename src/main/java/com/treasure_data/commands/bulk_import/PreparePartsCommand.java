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
package com.treasure_data.commands.bulk_import;

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.Command;
import com.treasure_data.commands.CommandContext;
import com.treasure_data.commands.CommandException;

public class PreparePartsCommand extends
        Command<PreparePartsRequest, PreparePartsResult> {
    private static final Logger LOG = Logger
            .getLogger(PreparePartsCommand.class.getName());

    @Override
    public void execute(
            CommandContext<PreparePartsRequest, PreparePartsResult> context)
            throws CommandException {
        LOG.fine(context.getRequest().getName() + " command started");

        // TODO

        LOG.fine(context.getRequest().getName() + " command finished");
    }

    public static void main(String[] args) throws Exception {
        Properties props = System.getProperties();

        PreparePartsCommand command = new PreparePartsCommand();
        PreparePartsRequest request = new PreparePartsRequest();
        PreparePartsResult result = new PreparePartsResult();
        CommandContext<PreparePartsRequest, PreparePartsResult> context =
                new CommandContext<PreparePartsRequest, PreparePartsResult>(
                        props, request, result);

        command.execute(context);
        LOG.info("exit " + result.getErrorCode());
    }
}
