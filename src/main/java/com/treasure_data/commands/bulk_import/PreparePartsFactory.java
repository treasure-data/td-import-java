package com.treasure_data.commands.bulk_import;

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.Config;

public class PreparePartsFactory {
    private static final Logger LOG = Logger
            .getLogger(PreparePartsFactory.class.getName());

    public static PreparePartsRequest newInstance(String[] fileNames,
            Properties props) throws CommandException {
        // format
        String f = props.getProperty(Config.BI_PREPARE_PARTS_FORMAT,
                Config.BI_PREPARE_PARTS_FORMAT_DEFAULTVALUE);
        PreparePartsRequest.Format format = PreparePartsRequest.Format
                .fromString(f);
        if (format == null) {
            throw new CommandException("unsupported format: " + f);
        }

        if (format.equals(PreparePartsRequest.Format.CSV)
                || format.equals(PreparePartsRequest.Format.TSV)) {
            return new CSVPreparePartsRequest(format, fileNames, props);
        } else if (format.equals(PreparePartsRequest.Format.JSON)) {
            throw new CommandException(new UnsupportedOperationException(
                    format.format()));
        } else if (format.equals(PreparePartsRequest.Format.MSGPACK)) {
            throw new CommandException(new UnsupportedOperationException(
                    format.format()));
        } else {
            throw new CommandException("fatal error: unsupported format: "
                    + format);
        }
    }
}
