package br.com.cursor.demo.data;

import br.com.cursor.demo.entity.DemoType;
import br.com.cursor.demo.util.MongoUtils;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.*;
import java.util.Random;

public class DataGenerator {

    private int instanceId = 0;

    private final int numberOfTypes;
    private final Double minInstancesToGenerate;
    private final MongoUtils mongoUtils;

    public DataGenerator(int numberOfTypes) {
        this.numberOfTypes = numberOfTypes;
        this.minInstancesToGenerate = (numberOfTypes - numberOfTypes * 0.8);
        this.mongoUtils = MongoUtils.getInstance();
    }

    public void generateTypes(final String fileName) throws IOException {
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "UTF-8"))){
            for(int counter = 0; counter < numberOfTypes; counter++) {
                writer.write(String.format("namespace%d type%d\n", counter, counter));
            }
        }
    }

    public int generateInstances(final DemoType demoType, final String databaseName, final String collectionName) {
        MongoCollection collection = mongoUtils.getCollection(collectionName, mongoUtils.getDatabase(databaseName));
        int instancesToGenerate = new Random().nextInt((numberOfTypes * 1000) - minInstancesToGenerate.intValue()) + minInstancesToGenerate.intValue();
        System.out.println(String.format("Generating %d instances to %s %s", instancesToGenerate, demoType.getNamespace(), demoType.getType()));
        for(int counter = 0; counter < instancesToGenerate; counter++, instanceId++) {
            collection.insertOne(createDocument(instanceId, demoType.getNamespace(), demoType.getType()));
        }
        return instancesToGenerate;
    }

    private Document createDocument(int instanceId, String namespace, String type) {
        Document document = new Document();
        document.put("id", instanceId);
        document.put("namespace", String.format("%s", namespace));
        document.put("type", String.format("%s",type));
        document.put("processed",  false);
        return document;
    }

}
