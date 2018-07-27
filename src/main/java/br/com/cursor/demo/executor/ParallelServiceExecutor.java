package br.com.cursor.demo.executor;

import br.com.cursor.demo.data.DataRetriever;
import br.com.cursor.demo.entity.DemoType;
import br.com.cursor.demo.executor.task.BatchGeneratorTask;
import br.com.cursor.demo.util.FileUtils;
import br.com.cursor.demo.util.MongoUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ParallelServiceExecutor extends Executor {

    private final int batchSize;

    public ParallelServiceExecutor(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void run() throws IOException {
        System.out.println("Starting the Service Executor parallel batch processing from cursor:");
        this.generateData();
        final DataRetriever dataRetriever = new DataRetriever(MongoUtils.DATABASE_NAME, MongoUtils.COLLECTION_NAME);
         Files.readAllLines(Paths.get(FileUtils.TYPES_FILE_NAME)).stream()
                .map(DemoType::getDemoType)
                .forEach(s -> this.demoTypeProcess(s, dataRetriever));
         TypeProcessorExecutorService.getInstance().waitExecutionToFinish();
        System.out.println(String.format("Finishing the parallel batch processing: Processed %d registers.",
                TypeProcessorExecutorService.getInstance().getProcessedTasks()));
        this.clearData();
    }

    private void demoTypeProcess(final DemoType demoType, final DataRetriever dataRetriever) {
        final BatchGeneratorTask batchGeneratorTask = new BatchGeneratorTask(demoType, dataRetriever, batchSize);
        TypeProcessorExecutorService.getInstance().enqueueTask(batchGeneratorTask);
    }
}
