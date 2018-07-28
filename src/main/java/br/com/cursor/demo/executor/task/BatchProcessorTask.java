package br.com.cursor.demo.executor.task;

import br.com.cursor.demo.data.DataUpdater;
import br.com.cursor.demo.entity.BatchJob;
import br.com.cursor.demo.executor.TypeProcessorExecutorService;
import br.com.cursor.demo.util.MongoUtils;

public class BatchProcessorTask implements ExporterTask {

    private BatchJob batchJob;

    public BatchProcessorTask(final BatchJob batchJob) {
        this.batchJob = batchJob;
    }

    @Override
    public void run() {
        TypeProcessorExecutorService.getInstance().incrementRunningTasks();
        this.processBatch();
        TypeProcessorExecutorService.getInstance().incrementProcessedTasks();
    }

    private void processBatch() {
        System.out.println(String.format("Processing batch job %d of size %d for namespace %s and type %s", batchJob.getBatchId(),
                batchJob.getIds().length, batchJob.getDemoType().getNamespace(), batchJob.getDemoType().getType()));
        final DataUpdater dataUpdater = new DataUpdater(MongoUtils.DATABASE_NAME, MongoUtils.COLLECTION_NAME);
        final int[] batchJobIds = batchJob.getIds();
        for (int batchJobId : batchJobIds) {
            if ((batchJob.getBatchId() % 10 == 0 && batchJobId % 100 == 0)) {
                try {
                    System.out.println(String.format("Long running batch job id %d for batch job %d of size %d for namespace %s and type %s", batchJobId, batchJob.getBatchId(),
                            batchJob.getIds().length, batchJob.getDemoType().getNamespace(), batchJob.getDemoType().getType()));
                    Thread.sleep(5001);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            dataUpdater.updateDemoTypeToProcessed(batchJobId);
        }
        System.out.println(String.format("Finished processing batch job %d of size %d for namespace %s and type %s", batchJob.getBatchId(),
                batchJob.getIds().length, batchJob.getDemoType().getNamespace(), batchJob.getDemoType().getType()));
    }
}
