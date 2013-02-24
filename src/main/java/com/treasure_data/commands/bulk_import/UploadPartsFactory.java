package com.treasure_data.commands.bulk_import;

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.Command;
import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.MultithreadsCommand;

public class UploadPartsFactory {
    private static final Logger LOG = Logger
            .getLogger(UploadPartsFactory.class.getName());

    private static final String PREPARE_PARTS_OPTION = "td.bulk_import.prepare_parts";

    public static UploadPartsRequest newRequestInstance(String sessionName,
            String[] fileNames, Properties props) throws CommandException {

        boolean autoPrepare = false;
        for (Object key : props.keySet()) {
            String k = (String) key;
            String val = props.getProperty(k);
            if (val != null && k.startsWith(PREPARE_PARTS_OPTION)) {
                autoPrepare = true;
                break;
            }
        }

        if (autoPrepare) {
            return new PrepareUploadPartsRequest(sessionName, fileNames, props);
        } else {
            return new UploadPartsRequest(sessionName, fileNames, props);
        }
    }

    public static UploadPartsResult newResultInstance(UploadPartsRequest request)
            throws CommandException {
        if (request instanceof PrepareUploadPartsRequest) {
            return new PrepareUploadPartsResult();
        } else if (request instanceof UploadPartsRequest) {
            return new UploadPartsResult();
        } else {
            String cname = request.getName();
            throw new CommandException("the type of this request is invalid: "
                    + cname);
        }
    }

    public static Command newCommandInstance(
            UploadPartsRequest request) throws CommandException {
        if (request instanceof PrepareUploadPartsRequest) {
            return new PrepareUploadPartsCommand();
        } else if (request instanceof UploadPartsRequest) {
            UploadPartsCommand<UploadPartsRequest, UploadPartsResult> command =
                    new UploadPartsCommand<UploadPartsRequest, UploadPartsResult>();
            return new MultithreadsCommand<UploadPartsRequest, UploadPartsResult>(command);
        } else {
            String cname = request.getName();
            throw new CommandException("the type of this request is invalid: "
                    + cname);
        }
    }
}
