//set MYPATH=C:\BDS2
//javac -g -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% %MYPATH%\MongoDB\Categorie.java
//java -Xmx256m -Xms256m -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% MongoDB.Categorie
package MongoDB;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DB;
import org.bson.Document;
import java.util.Arrays;
import java.util.List;
import com.mongodb.client.FindIterable;
import java.util.Iterator;
import java.time.LocalDate;
import java.util.ArrayList;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.model.UpdateOptions;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Categorie {
    private MongoDatabase database;
    private String dbName = "Library";
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ulib";
    private String passWord = "passUlib";
    private String categorieCollectionName = "colCategories";

    public String getCategorieCollectionName() {
        return this.categorieCollectionName;
    }

    Categorie() {
        MongoClient mongoClient = new MongoClient(hostName, port);
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("Connected to the database successfully");
        System.out.println("Credentials ::" + credential);
        database = mongoClient.getDatabase(dbName);
    }

    public void createCollectionCategories(String collectionName) {
        database.createCollection(collectionName);
        System.out.println("Collection Categories created successfully");
    }

    public void dropCollectionCategories(String collectionName) {
        MongoCollection<Document> colCategories = database.getCollection(collectionName);
        if (colCategories != null) {
            colCategories.drop();
            System.out.println("Collection colCategories removed successfully");
        }
    }

    public void insertOneCategorie(String collectionName, Document categorie) {
        MongoCollection<Document> colCategories = database.getCollection(collectionName);
        colCategories.insertOne(categorie);
        System.out.println("Document inserted successfully");
    }

    public void insertManyCategories(String collectionName, List<Document> categories) {
        MongoCollection<Document> colCategories = database.getCollection(collectionName);
        colCategories.insertMany(categories);
        System.out.println("Many Documents inserted successfully");
    }

    public List<Document> findCategories(String collectionName, Document filter) {
        MongoCollection<Document> colCategories = database.getCollection(collectionName);
        FindIterable<Document> iterable = filter == null ? colCategories.find() : colCategories.find(filter);

        List<Document> results = new ArrayList<>();
        for (Document document : iterable) {
            results.add(document);
        }
        return results;
    }

    public void updateCategorie(String collectionName, Document filter, Document update) {
        MongoCollection<Document> colCategories = database.getCollection(collectionName);
        UpdateResult result = colCategories.updateOne(filter, new Document("$set", update));
        System.out.println("Number of documents updated : " + result.getModifiedCount());
    }

    public void deleteCategorie(String collectionName, Document filter) {
        MongoCollection<Document> colCategories = database.getCollection(collectionName);
        colCategories.deleteOne(filter);
        System.out.println("Document deleted successfully");
    }

    public void printAllCategories(String collectionName) {
        List<Document> categories = findCategories(collectionName, null);
        for (Document category : categories) {
            System.out.println(category.toJson());
        }
    }

    public void insertJsonData(String collectionName, String jsonFileName) {
        String jsonFilePath = Paths.get(System.getenv("MYPATH"), "data", jsonFileName).toString();
        try {
            String content = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
            List<Document> categorieDocuments = new ArrayList<>();

            // Assuming the content string represents a JSON array, like: [{"key": "value"},
            // ...]
            content = content.trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                content = content.substring(1, content.length() - 1); // Remove the [ and ]
                String[] jsonObjects = content.split("},\\s*\\{");

                for (String jsonObject : jsonObjects) {
                    jsonObject = jsonObject.trim();
                    if (!jsonObject.startsWith("{"))
                        jsonObject = "{" + jsonObject;
                    if (!jsonObject.endsWith("}"))
                        jsonObject = jsonObject + "}";

                    Document categorieDoc = Document.parse(jsonObject);
                    categorieDocuments.add(categorieDoc);
                }
            }

            insertManyCategories(collectionName, categorieDocuments);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createIndexOnId() {
        MongoCollection<Document> colCategories = database.getCollection(categorieCollectionName);
        colCategories.createIndex(new Document("_id", 1));
    }

    public void createIndexOnNom() {
        MongoCollection<Document> colCategories = database.getCollection(categorieCollectionName);
        colCategories.createIndex(new Document("Nom", 1));
    }

    public static void main(String[] args) {
        Categorie categorie = new Categorie();
        categorie.dropCollectionCategories(categorie.categorieCollectionName);
        categorie.createCollectionCategories(categorie.categorieCollectionName);

        categorie.createIndexOnId();
        categorie.createIndexOnNom();

        categorie.insertJsonData(categorie.categorieCollectionName, "categorie_data.json");

        categorie.printAllCategories(categorie.categorieCollectionName);
    }

}
