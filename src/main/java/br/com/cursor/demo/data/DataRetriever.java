package br.com.cursor.demo.data;

import br.com.cursor.demo.entity.BatchJob;
import br.com.cursor.demo.entity.DemoType;
import br.com.cursor.demo.util.MongoUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataRetriever {

    private final String databaseName;
    private final String collectionName;
    private final MongoUtils mongoUtils;

    public DataRetriever(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.mongoUtils = MongoUtils.getInstance();
    }

    public List<BatchJob> retrieveBatchJobsByType(final DemoType demoType, int batchSize) {
        List<BatchJob> batchJobs = new ArrayList<>();
        List<Integer> instanceIds = retrieveIdsFromMongo(demoType);
        System.out.println(String.format("Retrieving batch jobs for %s %s.", demoType.getNamespace(), demoType.getType()));
        if (instanceIds.size() > 0) {
            composeBatchList(demoType, batchSize, batchJobs, instanceIds);
            return batchJobs;

        } else {
            return Collections.emptyList();
        }
    }

    private void composeBatchList(DemoType demoType, int batchSize, List<BatchJob> batchJobs, List<Integer> instanceIds) {
        int beginIndex = 0;
        int iter = 0;
        while (beginIndex < instanceIds.size()) {
            final int currentBatchSize = instanceIds.size() - beginIndex < batchSize ? instanceIds.size() - beginIndex : batchSize;
            batchJobs.add(getBatchJob(iter, demoType, beginIndex, beginIndex + currentBatchSize, instanceIds));
            beginIndex += currentBatchSize;
            iter++;
        }
        System.out.println(String.format("Retrieved %d batch jobs for %s %s with a total of %d ids.",
                batchJobs.size(), demoType.getNamespace(), demoType.getType(), instanceIds.size()));
    }


    private BatchJob getBatchJob(final int iter, final DemoType demoType, final int startIndex, final int endIndex, List<Integer> idList) {
        final int[] arrayId = new int[endIndex - startIndex];
        for(int counter = startIndex; counter < endIndex; counter++) {
            arrayId[counter - startIndex] = idList.get(counter);
        }
        return new BatchJob(iter, demoType, arrayId);
    }

    private List<Integer> retrieveIdsFromMongo(DemoType demoType) {
        final MongoCollection collection = mongoUtils.getCollection(collectionName, mongoUtils.getDatabase(databaseName));
        final BasicDBObject criteria = new BasicDBObject("namespace", demoType.getNamespace()).append("type", demoType.getType());
        final FindIterable<Document> documentIds = collection.find(criteria).projection(Projections.fields(Projections.include("id")));
        final List<Integer> idsList = new ArrayList<>();
        for(Document docId : documentIds) {
            idsList.add(docId.getInteger("id"));
        }
        return idsList;
    }
}
