package com.treasure_data.commands.bulk_import;

import com.treasure_data.commands.CommandResult;

public class PrepareUploadPartsResult extends CommandResult {

    private PreparePartsResult prepareResult;
    private UploadPartsResult uploadResult;

    public PreparePartsResult getPreparePartsResult() {
        return prepareResult;
    }

    public UploadPartsResult getUploadPartsResult() {
        return uploadResult;
    }
}
