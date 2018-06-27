package br.com.cursor.demo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.*;
import java.util.Random;

public class DataGenerator {

    private String fileName;
    private int numberOfTypes;
    private Double minInstancesToGenerage;
    private MongoClient mongoClient;
    private final String DATABASE_NAME = "cursor-demo-db";
    private final String COLLECTION_NAME = "instances";
    private final String SERVER_ADDRESS = "localhost";
    private final int SERVER_PORT = 27017;
    private MongoDatabase database;
    private static int instanceId = 0;


    public DataGenerator(String fileName, int numberOfTypes) {
        this.fileName = fileName;
        this.numberOfTypes = numberOfTypes;
        this.minInstancesToGenerage = (numberOfTypes - numberOfTypes * 0.8);
    }

    public void generateTypes() throws IOException {
        try (
                Writer writer = new BufferedWriter(
                    new OutputStreamWriter(
                        new FileOutputStream(fileName), "UTF-8"
                    )
                )
        ){
            for(int counter = 0; counter < numberOfTypes; counter++) {
                writer.write(String.format("namespace%d type%d\n", counter, counter));
            }
        }
    }

    public int generateInstances() {
        MongoCollection collection = getMongoCollection();
        int generatedInstances = 0;
        Random r;
        for(int typeId = 0; typeId < numberOfTypes; typeId++) {
            r = new Random();
            int instancesToGenerate = r.nextInt(numberOfTypes - minInstancesToGenerage.intValue()) + minInstancesToGenerage.intValue();
            System.out.println(String.format("Generating %d instances to namespace%d type%d", instancesToGenerate, typeId, typeId));
            for(int counter = 0; counter < instancesToGenerate; counter++, instanceId++) {
                persistInstance(instanceId, typeId, typeId, collection);
            }
            generatedInstances += instancesToGenerate;
        }
        mongoClient.close();
        return generatedInstances;
    }

    public void persistInstance(int instanceId, int namespaceId, int typeId, MongoCollection collection) {
        Document document = new Document();
        document.put("namespace", String.format("namespace%d", namespaceId));
        document.put("type", String.format("type%d", typeId));
        document.put("id", instanceId);
        collection.insertOne(document);
    }

    private MongoCollection<Document> getMongoCollection() {
        mongoClient = new MongoClient(SERVER_ADDRESS, SERVER_PORT);
        database = mongoClient.getDatabase(DATABASE_NAME);
        database.createCollection(COLLECTION_NAME);
        return database.getCollection(COLLECTION_NAME);
    }
}
