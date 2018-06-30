package br.com.cursor.demo.data;

import br.com.cursor.demo.util.MongoUtils;

import java.io.File;

public class DataCleaner {

    public boolean removeTypes(String fileName) {
        final File file = new File(fileName);
        return file.delete();
    }

    public boolean cleanDatabase(String databaseName) {
        MongoUtils.getInstance().getDatabase(databaseName).drop();
        return true;
    }
}
