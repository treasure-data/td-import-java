package com.treasure_data.commands.bulk_import;

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.CommandRequest;

public class PrepareUploadPartsRequest extends CommandRequest {
    private static final Logger LOG = Logger
            .getLogger(PrepareUploadPartsRequest.class.getName());

    private static final String COMMAND_NAME = "prepare_upload_parts";

    private PreparePartsRequest prepareRequest;
    private UploadPartsRequest uploadRequest;

    public PrepareUploadPartsRequest(Properties props)
            throws CommandException {
        super(props);
        // TODO
    }

    @Override
    public String getName() {
        return COMMAND_NAME;
    }

    public void createPreparePartsRequest() {
        // TODO
    }

    public PreparePartsRequest getPreparePartsRequest() {
        return prepareRequest;
    }

    public void createUploadPartsRequest() {
        // TODO
    }

    public UploadPartsRequest getUploadPartsRequest() {
        return uploadRequest;
    }
}
