package br.com.cursor.demo;

import br.com.cursor.demo.collector.FlattenListCollector;
import br.com.cursor.demo.data.DataCleaner;
import br.com.cursor.demo.data.DataGenerator;
import br.com.cursor.demo.data.DataRetriever;
import br.com.cursor.demo.data.DataUpdater;
import br.com.cursor.demo.entity.BatchJob;
import br.com.cursor.demo.entity.DemoType;
import br.com.cursor.demo.util.FileUtils;
import br.com.cursor.demo.util.MongoUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ParallelBatchesCursorDemo{
    private static final int BATCH_SIZE = 500;

    public static void main(String[] args)  throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println(String.format("Started to run at: %s", dateFormat.format(date)));
        ParallelBatchesCursorDemo parallelBatchesCursorDemo = new ParallelBatchesCursorDemo();
        parallelBatchesCursorDemo.run();
        MongoUtils.getInstance().createCollection(MongoUtils.COLLECTION_NAME, MongoUtils.getInstance().getDatabase(MongoUtils.DATABASE_NAME));
        // Mongo connections are shared so should destroy only when the program finishes to execute
        MongoUtils.getInstance().destroy();
        date = new Date();
        System.out.println(String.format("Finished to run at:  %s", dateFormat.format(date)));
    }

    public void run() throws IOException {
        System.out.println("Starting the parallel batch processing from cursor:");
        this.generateData();
        final DataRetriever dataRetriever = new DataRetriever(MongoUtils.DATABASE_NAME, MongoUtils.COLLECTION_NAME);
        List<BatchJob> batchJobs = getBatchJobs(dataRetriever);
        int processedCount = batchJobs
                .parallelStream()
                .mapToInt(s -> processBatch(s))
                .sum();

        System.out.println(String.format("Finishing the parallel batch processing: Processed %d registers.", processedCount));
        this.clearData();
    }

    private int processBatch(final BatchJob batchJob) {
        System.out.println(String.format("Processing batch job %d of size %d for namespace %s and type %s", batchJob.getBatchId(),
                batchJob.getIds().length, batchJob.getDemoType().getNamespace(), batchJob.getDemoType().getType()));
        final DataUpdater dataUpdater = new DataUpdater(MongoUtils.DATABASE_NAME, MongoUtils.COLLECTION_NAME);
        int[] batchJobIds = batchJob.getIds();
        int countUpdated = 0;
        for(int counter = 0; counter < batchJobIds.length; counter++) {
            if((Math.floor(Math.random() * 2) + 1) % 2 == 1) {
                dataUpdater.updateDemoTypToProcessed(batchJobIds[counter]);
                countUpdated++;
            }
        }
        return countUpdated;
    }

    private List<BatchJob> getBatchJobs(final DataRetriever dataRetriever) throws IOException {
        return Files.readAllLines(Paths.get(FileUtils.TYPES_FILE_NAME)).stream()
                .map(s -> DemoType.getDemoType(s))
                .map(s -> dataRetriever.retrieveBatchJobsByType(s, BATCH_SIZE))
                .collect(toFlattenBatchJobList());
    }

    private static FlattenListCollector<BatchJob> toFlattenBatchJobList() {
        return new FlattenListCollector<>();
    }

    public void generateData() throws IOException {
        System.out.println("Generating data for execution: ");
        final DataGenerator generator = new DataGenerator(FileUtils.NUMBER_OF_TYPES);
        generator.generateTypes(FileUtils.TYPES_FILE_NAME);
        int instancesGenerated = Files.readAllLines(Paths.get(FileUtils.TYPES_FILE_NAME))
                .parallelStream()
                .map(s -> DemoType.getDemoType(s))
                .mapToInt(s -> generator.generateInstances(s, MongoUtils.DATABASE_NAME, MongoUtils.COLLECTION_NAME))
                .sum();
        System.out.println(String.format("%d types generated. %d instances generated.", FileUtils.NUMBER_OF_TYPES, instancesGenerated));
    }

    public void clearData() {
        System.out.println("Clearing data for execution:");
        final DataCleaner cleaner = new DataCleaner();
        int fileCount = cleaner.removeTypes(FileUtils.TYPES_FILE_NAME) ? 1 : 0;
        int databaseCount = cleaner.cleanDatabase(MongoUtils.DATABASE_NAME) ? 1 : 0;
        System.out.println(String.format("Finished cleaning data for execution. %d database removed. %d files removed.", fileCount, databaseCount));
    }

}
