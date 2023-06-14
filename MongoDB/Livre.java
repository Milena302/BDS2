package MongoDB;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.bson.Document;
import com.mongodb.client.FindIterable;
import java.util.Iterator;
import java.time.LocalDate;
import java.util.ArrayList;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.model.UpdateOptions;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.swing.tree.ExpandVetoException;
public class Livre {
    private MongoDatabase database; // Database instance, pointer
    private String dbName = "Library"; // Database name
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ulib";
    private String passWord = "passUlib";
    private String LivreCollectionName = "colLivres";

    Livre() {
        MongoClient mongoClient = new MongoClient(hostName, port);
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("Connected to the database successfully");
        System.out.println("Credentials ::" + credential);
        database = mongoClient.getDatabase(dbName);
    }

    public void createCollectionLivres(String collectionName) {
        database.createCollection(collectionName);
        System.out.println("Collection Livres created successfully");
    }

    public void dropCollectionLivres(String collectionName) {
        // Drop a collection
        MongoCollection<Document> colLivres = null;
        System.out.println("\n\n\n*********** in dropCollectionLivres *****************");
        colLivres = database.getCollection(collectionName);
        System.out.println("!!!! Collection Livres : " + colLivres);
        // If collection does not exist, it does not need to be dropped
        if (colLivres != null) {
            colLivres.drop();
            System.out.println("Collection colLivres removed successfully !!!");
        }
    }

    public void insertOneLivre(String collectionName, Document Livre) {
        // Drop a collection
        MongoCollection<Document> colLivres = database.getCollection(collectionName);
        colLivres.insertOne(Livre);
        System.out.println("Document inserted successfully");
    }

    public void insertManyLivres(String collectionName, List<Document> Livres) {
        // Drop a collection
        MongoCollection<Document> colLivres = database.getCollection(collectionName);
        colLivres.insertMany(Livres);
        System.out.println("Many Documents inserted successfully");
    }

    public List<Document> findLivres(String collectionName, Document filter) {
        MongoCollection<Document> colLivres = database.getCollection(collectionName);
        FindIterable<Document> iterable = filter == null ? colLivres.find() : colLivres.find(filter);

        List<Document> results = new ArrayList<>();
        for (Document document : iterable) {
            results.add(document);
        }
        return results;
    }

    public void updateLivre(String collectionName, Document filter, Document update) {
        MongoCollection<Document> colLivres = database.getCollection(collectionName);
        UpdateResult result = colLivres.updateOne(filter, new Document("$set", update));
        System.out.println("Number of documents updated : " + result.getModifiedCount());
    }

    public void deleteLivre(String collectionName, Document filter) {
        MongoCollection<Document> colLivres = database.getCollection(collectionName);
        colLivres.deleteOne(filter);
        System.out.println("Document deleted successfully");
    }

    public void insertJsonData(String collectionName, String jsonFilePath) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
            List<Document> livreDocuments = new ArrayList<>();

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

                    Document livreDoc = Document.parse(jsonObject);
                    livreDocuments.add(livreDoc);
                }
            }

            insertManyLivres(LivreCollectionName, livreDocuments);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Document> joinEditeursWithLivres(String editCol) {
    
        MongoCollection<Document> colLivres = database.getCollection(LivreCollectionName);

        List<Document> pipeline = Arrays.asList(
                new Document("$lookup",
                        new Document("from", editCol)
                                .append("localField", "Editeur")
                                .append("foreignField", "_id")
                                .append("as", "editeur")));

        List<Document> results = new ArrayList<>();
        for (Document document : colLivres.aggregate(pipeline)) {
            results.add(document);
        }
        return results;
    }
    
    public static void main(String[] args) {
        // Create instance of Livre class

        
        Livre livre = new Livre();
        Editeur editeur = new Editeur();


        // Drop collection if exists
        livre.dropCollectionLivres(livre.LivreCollectionName);

        // Create new collection
        livre.createCollectionLivres(livre.LivreCollectionName);

        livre.insertJsonData(livre.LivreCollectionName, "livre_data.json");

        List<Document> result = livre.joinEditeursWithLivres(editeur.getEditName());
         for (Document document : result) {
             System.out.println(document.toJson());
         }

    }
    
}

    


