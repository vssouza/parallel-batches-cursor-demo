package br.com.cursor.demo.data;

import br.com.cursor.demo.util.MongoUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;

public class DataUpdater {
    private final MongoUtils mongoUtils;
    private final String databaseName;
    private final String collectionName;

    public DataUpdater(String databaseName, String collectionName) {
        this.mongoUtils = MongoUtils.getInstance();
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public void updateDemoTypToProcessed(int id) {
        final MongoCollection collection = mongoUtils.getCollection(collectionName, mongoUtils.getDatabase(databaseName));
        BasicDBObject criteria = new BasicDBObject("_id", id);
        Document document = new Document();
        document.put("processed", true);
        Bson updatedDocument = new Document("$set", document);
        collection.findOneAndUpdate(criteria, updatedDocument);
    }
}
