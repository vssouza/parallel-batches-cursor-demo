package br.com.cursor.demo.executor;

import br.com.cursor.demo.data.DataUpdater;
import br.com.cursor.demo.entity.BatchJob;
import br.com.cursor.demo.util.MongoUtils;

public abstract class BatchExecutor extends Executor{

    int processBatch(final BatchJob batchJob) {
        System.out.println(String.format("Processing batch job %d of size %d for namespace %s and type %s", batchJob.getBatchId(),
                batchJob.getIds().length, batchJob.getDemoType().getNamespace(), batchJob.getDemoType().getType()));
        final DataUpdater dataUpdater = new DataUpdater(MongoUtils.DATABASE_NAME, MongoUtils.COLLECTION_NAME);
        final int[] batchJobIds = batchJob.getIds();
        int countUpdated = 0;
        for (int batchJobId : batchJobIds) {
//            if ((Math.floor(Math.random() * 2) + 1) % 2 == 1) {
                dataUpdater.updateDemoTypToProcessed(batchJobId);
                countUpdated++;
//            }
        }
        return countUpdated;
    }
}
