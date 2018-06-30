package br.com.cursor.demo;

import br.com.cursor.demo.data.DataCleaner;
import br.com.cursor.demo.data.DataUpdater;
import br.com.cursor.demo.entity.BatchJob;
import br.com.cursor.demo.util.FileUtils;
import br.com.cursor.demo.util.MongoUtils;

import java.io.IOException;

public abstract class Executor {

    public abstract void run() throws IOException;

    int processBatch(final BatchJob batchJob) {
        System.out.println(String.format("Processing batch job %d of size %d for namespace %s and type %s", batchJob.getBatchId(),
                batchJob.getIds().length, batchJob.getDemoType().getNamespace(), batchJob.getDemoType().getType()));
        final DataUpdater dataUpdater = new DataUpdater(MongoUtils.DATABASE_NAME, MongoUtils.COLLECTION_NAME);
        int[] batchJobIds = batchJob.getIds();
        int countUpdated = 0;
        for (int batchJobId : batchJobIds) {
            if ((Math.floor(Math.random() * 2) + 1) % 2 == 1) {
                dataUpdater.updateDemoTypToProcessed(batchJobId);
                countUpdated++;
            }
        }
        return countUpdated;
    }

    void clearData() {
        System.out.println("Clearing data for execution:");
        final DataCleaner cleaner = new DataCleaner();
        final int fileCount = cleaner.removeTypes(FileUtils.TYPES_FILE_NAME) ? 1 : 0;
        final int databaseCount = cleaner.cleanDatabase(MongoUtils.DATABASE_NAME) ? 1 : 0;
        System.out.println(String.format("Finished cleaning data for execution. %d database removed. %d files removed.", fileCount, databaseCount));
    }

    static Executor getExecutor(ExecutorType executorType, int batchSize) {
        switch (executorType) {
            case SEQUENTIAL:
                return new SequentialExecutor(batchSize);
            case PARALLEL:
                return new ParallelExecutor(batchSize);
            default:
                throw new UnsupportedOperationException("Invalid executor type provided.");
        }
    }


}
