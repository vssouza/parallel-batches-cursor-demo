package br.com.cursor.demo.util;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;

public class MongoUtils {
    public static final String DATABASE_NAME = "cursor-demo-db";
    public static final String COLLECTION_NAME = "instances";
    public static final String SERVER_ADDRESS = "localhost";
    public static final int SERVER_PORT = 27017;
    private static MongoUtils instance;
    private MongoClient mongoClient;

    private MongoUtils(){
        mongoClient = new MongoClient(SERVER_ADDRESS, SERVER_PORT);
    }

    public static MongoUtils getInstance() {
        if(instance == null) {
            instance = new MongoUtils();
        }
        return instance;
    }

    public MongoDatabase getDatabase(String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }

    public MongoCollection<Document> getCollection(String collectionName, MongoDatabase database) {
        return database.getCollection(collectionName);
    }

    public void destroy() {
        mongoClient.close();
    }
}
