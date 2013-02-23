package com.treasure_data.commands.bulk_import;

public class PrepareUploadPartsResult extends UploadPartsResult {

    //private PreparePartsResult prepareResult = new PreparePartsResult();
    private PreparePartsResult prepareResult = new MultiThreadsPreparePartsResult();

    public PreparePartsResult getPreparePartsResult() {
        return prepareResult;
    }

    public UploadPartsResult getUploadPartsResult() {
        return this;
    }

    public Object clone() {
        return new PrepareUploadPartsResult();
    }
}
