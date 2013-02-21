package com.treasure_data.commands.bulk_import;

public class PrepareUploadPartsResult extends UploadPartsResult {

    private PreparePartsResult prepareResult;

    public PreparePartsResult getPreparePartsResult() {
        return prepareResult;
    }

    public UploadPartsResult getUploadPartsResult() {
        return this;
    }
}
