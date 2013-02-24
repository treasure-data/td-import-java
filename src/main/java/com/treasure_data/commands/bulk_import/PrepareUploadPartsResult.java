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

    public static class MultiThreadsPreparePartsResult extends PreparePartsResult {

        public MultiThreadsPreparePartsResult() {
            super();
        }

        public synchronized void addOutputFilePath(String filePath) {
            super.addOutputFilePath(filePath);
            PrepareUploadPartsCommand.uploadTaskQueue.add(
                    new PrepareUploadPartsCommand.UploadWorker.Task(filePath));
        }

        public Object clone() {
            return new MultiThreadsPreparePartsResult();
        }

        public synchronized void addFinishTask(int numOfThreads) {
            for (int i = 0; i < numOfThreads; i++) {
                PrepareUploadPartsCommand.uploadTaskQueue
                .add(PrepareUploadPartsCommand.UploadWorker.FINISH_TASK);
            }
        }
    }
}
