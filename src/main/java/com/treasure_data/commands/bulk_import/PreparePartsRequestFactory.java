package com.treasure_data.commands.bulk_import;

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.Config;

public class PreparePartsRequestFactory {
    private static final Logger LOG = Logger
            .getLogger(PreparePartsRequestFactory.class.getName());

    public static PreparePartsRequest newInstance(String[] fileNames,
            Properties props) throws CommandException {
        // format
        String format = props.getProperty(Config.BI_PREPARE_PARTS_FORMAT,
                Config.BI_PREPARE_PARTS_FORMAT_DEFAULTVALUE);
        if (format.equals("csv") || format.equals("tsv")) {
            return new CSVPreparePartsRequest(format, fileNames, props);
        } else {
            throw new CommandException("unsupported format: " + format);
        }
    }
}
