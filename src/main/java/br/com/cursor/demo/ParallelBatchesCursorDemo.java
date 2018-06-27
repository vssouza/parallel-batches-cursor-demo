package br.com.cursor.demo;

import br.com.cursor.demo.entity.DataRetriever;
import br.com.cursor.demo.entity.DemoType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ParallelBatchesCursorDemo{
    private static final String TYPES_FILE_NAME = "types.txt";
    private static final int NUMBER_OF_TYPES = 100;
    private static final int BATCH_SIZE = 500;

    public static void main(String[] args)  throws Exception{
//        System.out.println("Starting the parallel batch processing from cursor:");
//        generateData();

        DataRetriever dataRetriever = new DataRetriever();

        // retrieve all types from a file and get the IDs from mongoDB
        List<List<Integer>> instanceIds =  Files.lines(Paths.get(TYPES_FILE_NAME)).map(
                s -> DemoType.getDemoType(s)
        ).map(
                s -> dataRetriever.retrieveIdsFromMongo(s).iterator()
        ).collect(toIdList());

        dataRetriever.close();

        // now we have the complete list of IDs to be retrieved so we split in batches and fire in parallel to threads



//        System.out.println("Finishing the parallel batch processing from cursor:");
//        clearData();
    }

    public static IdBatchCollector toIdList() {
        return new IdBatchCollector(BATCH_SIZE);
    }

    public static void generateData() throws IOException {
        System.out.println("Generating data for execution: ");
        DataGenerator generator = new DataGenerator(TYPES_FILE_NAME, NUMBER_OF_TYPES);
        generator.generateTypes();
        int numberOfInstances = generator.generateInstances();
        System.out.println(String.format("%d types generated. %d instances generage.", NUMBER_OF_TYPES, numberOfInstances));
    }

    public static void clearData() {
        System.out.println("Clearing data for execution:");
        DataCleaner cleaner = new DataCleaner();
        int fileCount = cleaner.removeTypes() ? 1 : 0;
        int databaseCount = cleaner.dropMongoDatabase() ? 1 : 0;
        System.out.println(String.format("Finished cleaning data for execution. %d database removed. %d files removed.", fileCount, databaseCount));
    }

}
