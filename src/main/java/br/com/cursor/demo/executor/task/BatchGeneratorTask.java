package br.com.cursor.demo.executor.task;

import br.com.cursor.demo.data.DataRetriever;
import br.com.cursor.demo.entity.BatchJob;
import br.com.cursor.demo.entity.DemoType;
import br.com.cursor.demo.executor.TypeProcessorExecutorService;

public class BatchGeneratorTask implements ExporterTask {

    private final DemoType demoType;
    private final DataRetriever dataRetriever;
    private final int batchSize;

    public BatchGeneratorTask(final DemoType demoType, final DataRetriever dataRetriever, final int batchSize) {
        this.demoType = demoType;
        this.dataRetriever = dataRetriever;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        TypeProcessorExecutorService.getInstance().incrementRunningTasks();
        dataRetriever.retrieveBatchJobsByType(demoType, batchSize).stream()
        .forEach(s -> enqueueBatchProcessorTask(s));
        TypeProcessorExecutorService.getInstance().incrementProcessedTasks();
    }

    private void enqueueBatchProcessorTask(final BatchJob batchJob) {
        final BatchProcessorTask batchProcessorTask = new BatchProcessorTask(batchJob);
        TypeProcessorExecutorService.getInstance().enqueueTask(batchProcessorTask);
    }
}
