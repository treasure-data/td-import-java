package com.treasure_data.commands.bulk_import;

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;

public class PrepareUploadPartsRequest extends UploadPartsRequest {
    private static final Logger LOG = Logger
            .getLogger(PrepareUploadPartsRequest.class.getName());

    private static final String COMMAND_NAME = "prepare_upload_parts";

    private PreparePartsRequest prepareRequest;

    public PrepareUploadPartsRequest() throws CommandException {
        super();
    }

    public PrepareUploadPartsRequest(String sessionName, String[] fileNames,
            Properties props) throws CommandException {
        super(sessionName, fileNames, props);
        prepareRequest = PreparePartsFactory.newInstance(fileNames, props);
    }

    @Override
    public String getName() {
        return COMMAND_NAME;
    }

    public PreparePartsRequest getPreparePartsRequest() {
        return prepareRequest;
    }

    public UploadPartsRequest getUploadPartsRequest() {
        return this;
    }
}
