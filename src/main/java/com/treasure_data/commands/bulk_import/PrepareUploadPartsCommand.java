package com.treasure_data.commands.bulk_import;

import java.io.File;
import java.util.List;
import java.util.logging.Logger;

import com.treasure_data.commands.CommandException;
import com.treasure_data.commands.MultithreadsCommand;

public class PrepareUploadPartsCommand extends
        UploadPartsCommand<PrepareUploadPartsRequest, PrepareUploadPartsResult> {
    private static final Logger LOG = Logger
            .getLogger(PrepareUploadPartsCommand.class.getName());

    public PrepareUploadPartsCommand() {
    }

    @Override
    public void execute(PrepareUploadPartsRequest request,
            PrepareUploadPartsResult result) throws CommandException {
        File[] files = request.getFiles();
        for (File f : files) {
            PrepareUploadPartsResult clonedResult =
                    (PrepareUploadPartsResult) result.clone();
            execute(request, clonedResult, f);
        }
    }

    @Override
    public void execute(PrepareUploadPartsRequest request,
            PrepareUploadPartsResult result, File file) throws CommandException {
        LOG.fine(String.format("started preparing file: %s", file.getName()));
        PreparePartsRequest prepareRequest = request.getPreparePartsRequest();
        PreparePartsResult prepareResult = result.getPreparePartsResult();
        PreparePartsCommand prepareCommand = new PreparePartsCommand();
        prepareCommand.execute(prepareRequest, prepareResult, file);
        List<String> filePaths = prepareResult.getOutputFiles();

        // TODO
        // TODO #MN can optimize the method with multi-threading
        // TODO

        LOG.fine(String.format("started uploading file: %s", file.getName()));
        UploadPartsRequest uploadRequest = request.getUploadPartsRequest();
        uploadRequest.setFiles(filePaths.toArray(new String[0]));
        UploadPartsResult uploadResult = result.getUploadPartsResult();
        UploadPartsCommand uploadCommand = new UploadPartsCommand();
        MultithreadsCommand<UploadPartsRequest, UploadPartsResult> multithreading =
                new MultithreadsCommand<UploadPartsRequest, UploadPartsResult>(uploadCommand);
        multithreading.execute(uploadRequest, uploadResult);

    }
}
