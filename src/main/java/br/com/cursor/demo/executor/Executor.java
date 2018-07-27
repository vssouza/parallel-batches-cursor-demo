package br.com.cursor.demo.executor;

import br.com.cursor.demo.data.DataCleaner;
import br.com.cursor.demo.data.DataGenerator;
import br.com.cursor.demo.data.DataUpdater;
import br.com.cursor.demo.entity.BatchJob;
import br.com.cursor.demo.entity.DemoType;
import br.com.cursor.demo.util.FileUtils;
import br.com.cursor.demo.util.MongoUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public abstract class Executor {

    public abstract void run() throws IOException;

    void clearData() {
        System.out.println("Clearing data for execution:");
        final DataCleaner cleaner = new DataCleaner();
        final int fileCount = cleaner.removeTypes(FileUtils.TYPES_FILE_NAME) ? 1 : 0;
        final int databaseCount = cleaner.cleanDatabase(MongoUtils.DATABASE_NAME) ? 1 : 0;
        System.out.println(String.format("Finished cleaning data for execution. %d database removed. %d files removed.", fileCount, databaseCount));
    }

    public static Executor getExecutor(ExecutorType executorType, int batchSize) {
        switch (executorType) {
            case SEQUENTIAL:
                return new SequentialExecutor(batchSize);
            case PARALLEL:
                return new ParallelExecutor(batchSize);
            case PARALLEL_EXECUTOR_SERVICE:
                return new ParallelServiceExecutor(batchSize);
            default:
                throw new UnsupportedOperationException("Invalid executor type provided.");
        }
    }

    void generateData() throws IOException {
        System.out.println("Generating data in parallel for execution: ");
        final DataGenerator generator = new DataGenerator(FileUtils.NUMBER_OF_TYPES);
        generator.generateTypes(FileUtils.TYPES_FILE_NAME);
        int instancesGenerated = Files.readAllLines(Paths.get(FileUtils.TYPES_FILE_NAME))
                .parallelStream()
                .map(DemoType::getDemoType)
                .mapToInt(s -> generator.generateInstances(s, MongoUtils.DATABASE_NAME, MongoUtils.COLLECTION_NAME))
                .sum();
        System.out.println(String.format("%d types generated. %d instances generated.", FileUtils.NUMBER_OF_TYPES, instancesGenerated));
    }
}
