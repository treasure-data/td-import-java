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
package com.treasure_data.bulk_import;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import joptsimple.HelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;


public class BulkImportOptions {
    private static final Logger LOG = Logger.getLogger(BulkImportOptions.class.getName());

    private static class SimpleHelpFormatter implements HelpFormatter {
        private int DESCRIPTION_INDENT = 37;
        private int DESCRIPTION_LIMIT = 60;

        public String format(Map<String, ? extends OptionDescriptor> options) {
            StringBuilder sb = new StringBuilder();

            Set<OptionDescriptor> used = new HashSet<OptionDescriptor>();

            for(OptionDescriptor desc : options.values()) {
                if(desc.representsNonOptions()) {
                    continue;
                }

                if(used.contains(desc)) {
                    continue;
                }
                used.add(desc);

                if(desc.options().contains("help")) {
                    // hide --help
                    continue;
                }

                int blen = sb.length();

                sb.append("    ");

                int n = 0;
                for(String s : desc.options()) {
                    if(n != 0) {
                        sb.append(", ");
                    }
                    if(s.length() > 1) {
                        sb.append("--");
                    } else {
                        sb.append("-");
                    }
                    sb.append(s);
                    n++;
                }

                if(desc.acceptsArguments()) {
                    sb.append(" ");
                    if(!desc.requiresArgument()) {
                        sb.append("[");
                    }

                    String arg = desc.argumentDescription();
                    if(arg == null && arg.isEmpty()) {
                        arg = desc.argumentTypeIndicator();
                        if(arg == null && arg.isEmpty()) {
                            arg = "string";
                        }
                    }
                    sb.append(arg);

                    if(!desc.requiresArgument()) {
                        sb.append("]");
                    }
                }

                int length = sb.length() - blen;

                if(length >= DESCRIPTION_INDENT-2) {
                    sb.append("\n");
                    for(int i=0; i < DESCRIPTION_INDENT-1; i++) {
                        sb.append(" ");
                    }
                } else {
                    for(int i=length; i < DESCRIPTION_INDENT-1; i++) {
                        sb.append(" ");
                    }
                }

                String line = desc.description();
                String[] words = line.split(" ");

                blen = sb.length();
                for(String word : words) {
                    if(sb.length() - blen + word.length() > DESCRIPTION_LIMIT) {
                        sb.append("\n");
                        for(int i=0; i < DESCRIPTION_INDENT; i++) {
                            sb.append(" ");
                        }
                        blen = sb.length();
                        sb.append("  ");
                    } else {
                        sb.append(" ");
                    }
                    sb.append(word);
                }

                sb.append("\n");
            }

            return sb.toString();
        }
    }

    protected OptionParser op;
    protected OptionSet options;

    public BulkImportOptions() {
        op = new OptionParser();
    }

    public void initPrepareOptionParser(Properties props) throws Exception {
        op.formatHelpWith(new SimpleHelpFormatter());
        op.acceptsAll(Arrays.asList("h", "help"),
                "show this help message");

        op.acceptsAll(Arrays.asList("f", "format"),
                "source file format [csv, tsv, json, msgpack]; default=csv")
                .withRequiredArg()
                .describedAs("FORMAT")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("C", "compress"),
                "compressed type [gzip, none]; default=auto detect")
                .withRequiredArg()
                .describedAs("TYPE")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("e", "encoding"),
                "encoding type [utf-8]")
                .withRequiredArg()
                .describedAs("TYPE")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("t", "time-column"),
                "name of the time column")
                .withRequiredArg()
                .describedAs("NAME")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("T", "time-format"),
                "STRF_FORMAT; default=auto detect")
                .withRequiredArg()
                .withValuesSeparatedBy(",")
                .describedAs("FORMAT")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("time-value"),
                "long value of the time column")
                .withRequiredArg()
                .describedAs("TIME")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("o", "output"),
                "output directory")
                .withRequiredArg()
                .describedAs("DIR")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("s", "split-size"),
                "size of each parts (default: 16384)")
                .withRequiredArg()
                .describedAs("SIZE_IN_KB")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("error-records-handling"),
                "error records handling mode [skip, abort]; default=skip")
                .withRequiredArg()
                .describedAs("MODE")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("delimiter"),
                "delimiter CHAR; default=\",\" at csv, \"\\t\" at tsv")
                .withRequiredArg()
                .describedAs("CHAR")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("quote"),
                "quote [DOUBLE, SINGLE]; default=DOUBLE")
                .withRequiredArg()
                .describedAs("CHAR")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("newline"),
                "newline [CRLR, LR, CR];  default=CRLF")
                .withRequiredArg()
                .describedAs("TYPE")
                .ofType(String.class);
        op.acceptsAll(Arrays.asList("column-header"),
                "first line includes column names")
                .withOptionalArg();
        op.acceptsAll(Arrays.asList("columns"),
                "column names (use --column-header instead if the first line has column names)")
                .withRequiredArg()
                .describedAs("NAME,NAME,...")
                .ofType(String.class)
                .withValuesSeparatedBy(",");
        op.acceptsAll(Arrays.asList("column-types"),
                "column types [string, int, long]")
                .withRequiredArg()
                .describedAs("TYPE,TYPE,...")
                .ofType(String.class)
                .withValuesSeparatedBy(",");
        op.acceptsAll(Arrays.asList("exclude-columns"),
                "exclude columns")
                .withRequiredArg()
                .describedAs("NAME,NAME,...")
                .ofType(String.class)
                .withValuesSeparatedBy(",");
        op.acceptsAll(Arrays.asList("only-columns"),
                "only-columns")
                .withRequiredArg()
                .describedAs("NAME,NAME,...")
                .ofType(String.class)
                .withValuesSeparatedBy(",");
        op.acceptsAll(Arrays.asList("prepare-parallel"),
                "prepare in parallel (default: 2; max 8)")
                .withRequiredArg()
                .describedAs("NUM")
                .ofType(String.class);

    }

    public void initUploadOptionParser(Properties props) throws Exception {
        this.initPrepareOptionParser(props);
        op.acceptsAll(Arrays.asList("auto-perform"), // TODO
                "perform bulk import job automatically")
                .withOptionalArg();
        op.acceptsAll(Arrays.asList("auto-commit"), // TODO
                "commit bulk import job automatically")
                .withOptionalArg();
        op.acceptsAll(Arrays.asList("parallel"),
                "upload in parallel (default: 2; max 8)")
                .withRequiredArg()
                .describedAs("NUM")
                .ofType(String.class);
    }

    public void setOptions(final String[] args) {
        options = op.parse(args);
        // TODO
        //@SuppressWarnings("unchecked")
        //List<String> nonOptArgs = (List<String>) options.nonOptionArguments();
    }

    public OptionSet getOptions() {
        return options;
    }
}
