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
            if (batchJob.getBatchId() % 10 == 0 && batchJobId % 100 == 0) {
                try {
                    System.out.println(String.format("Long running batch job id %d for batch job %d of size %d for namespace %s and type %s", batchJobId, batchJob.getBatchId(),
                            batchJob.getIds().length, batchJob.getDemoType().getNamespace(), batchJob.getDemoType().getType()));
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            dataUpdater.updateDemoTypeToProcessed(batchJobId);
            countUpdated++;
        }
        System.out.println(String.format("Finished processing batch job %d of size %d for namespace %s and type %s", batchJob.getBatchId(),
                batchJob.getIds().length, batchJob.getDemoType().getNamespace(), batchJob.getDemoType().getType()));
        return countUpdated;
    }
}
