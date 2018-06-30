package br.com.cursor.demo;

import br.com.cursor.demo.util.MongoUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ParallelBatchesDemo {
    private static final int BATCH_SIZE = 500;
    private static ExecutorType EXECUTOR_TYPE = ExecutorType.PARALLEL;


    public static void main(String[] args)  throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println(String.format("Started to run at: %s", dateFormat.format(date)));
        Executor.getExecutor(EXECUTOR_TYPE, BATCH_SIZE).run();
        MongoUtils.getInstance().createCollection(MongoUtils.COLLECTION_NAME, MongoUtils.getInstance().getDatabase(MongoUtils.DATABASE_NAME));
        // Mongo connections are shared so should destroy only when the program finishes to execute
        MongoUtils.getInstance().destroy();
        date = new Date();
        System.out.println(String.format("Finished to run at:  %s", dateFormat.format(date)));
    }
}
