package br.com.cursor.demo.entity;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class DataRetriever {

    private MongoClient mongoClient;
    private final String DATABASE_NAME = "cursor-demo-db";
    private final String COLLECTION_NAME = "instances";
    private final String SERVER_ADDRESS = "localhost";
    private final int SERVER_PORT = 27017;
    private MongoDatabase database;

    public DataRetriever() {
        mongoClient = new MongoClient(SERVER_ADDRESS, SERVER_PORT);
        database = mongoClient.getDatabase(DATABASE_NAME);
        database.getCollection(COLLECTION_NAME);
    }

    public FindIterable<Document> retrieveIdsFromMongo(DemoType demoType) {
        MongoCollection collection = getCollection();
        BasicDBObject criteria = new BasicDBObject("namespace", demoType.getNamespace()).append("type", demoType.getType());
        return collection.find(criteria).projection(Projections.fields(Projections.include("id")));
    }

    public void close() {
        mongoClient.close();
    }

    private MongoCollection<Document> getCollection() {
        return database.getCollection(COLLECTION_NAME);

    }
}
