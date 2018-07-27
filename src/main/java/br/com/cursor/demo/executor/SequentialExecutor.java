package br.com.cursor.demo.executor;

import br.com.cursor.demo.collector.FlattenListCollector;
import br.com.cursor.demo.data.DataGenerator;
import br.com.cursor.demo.data.DataRetriever;
import br.com.cursor.demo.entity.BatchJob;
import br.com.cursor.demo.entity.DemoType;
import br.com.cursor.demo.util.FileUtils;
import br.com.cursor.demo.util.MongoUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class SequentialExecutor extends BatchExecutor {

    private int batchSize;

    SequentialExecutor(int batchSize) {
        this.batchSize = batchSize;
    }

    public void run() throws IOException {
        System.out.println("Starting the sequential batch processing from cursor:");
        this.generateData();
        final DataRetriever dataRetriever = new DataRetriever(MongoUtils.DATABASE_NAME, MongoUtils.COLLECTION_NAME);
        List<BatchJob> batchJobs = getBatchJobs(dataRetriever);
        int processedCount = batchJobs
                .stream()
                .mapToInt(this::processBatch)
                .sum();

        System.out.println(String.format("Finishing the sequential batch processing: Processed %d registers.", processedCount));
        this.clearData();
    }

    private List<BatchJob> getBatchJobs(final DataRetriever dataRetriever) throws IOException {
        return Files.readAllLines(Paths.get(FileUtils.TYPES_FILE_NAME)).stream()
                .map(DemoType::getDemoType)
                .map(s -> dataRetriever.retrieveBatchJobsByType(s, batchSize))
                .collect(toFlattenBatchJobList());
    }

    private static FlattenListCollector<BatchJob> toFlattenBatchJobList() {
        return new FlattenListCollector<>();
    }
}

