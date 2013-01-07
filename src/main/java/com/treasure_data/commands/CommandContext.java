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
package com.treasure_data.commands;

import java.util.Properties;

public class CommandContext<REQ extends CommandRequest, RET extends CommandResult> {
    private Properties props;
    private REQ request;
    private RET result;

    public CommandContext(Properties props, REQ request, RET result) {
        this.props = props;
        this.request = request;
        this.result = result;
    }

    public Properties getProperties() {
        return props;
    }

    public REQ getRequest() {
        return request;
    }

    public RET getResult() {
        return result;
    }
}
