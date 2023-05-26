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

    public static void main(String[] args) {
        // Create instance of Livre class
        Livre livre = new Livre();

        // Drop collection if exists
        livre.dropCollectionLivres(livre.LivreCollectionName);

        // Create new collection
        livre.createCollectionLivres(livre.LivreCollectionName);

        // Create a new document
        Document doc = new Document("title", "The Great Gatsby")
                .append("author", "F. Scott Fitzgerald")
                .append("year", 1925);

        // Insert the document into the collection
        livre.insertOneLivre(livre.LivreCollectionName, doc);

        // Update the document
        Document update = new Document("year", 1926);
        Document filter = new Document("title", "The Great Gatsby");
        livre.updateLivre(livre.LivreCollectionName, filter, update);

        // Read the updated document
        List<Document> results = livre.findLivres(livre.LivreCollectionName, filter);
        for (Document document : results) {
            System.out.println(document.toJson());
        }

        // Delete the document
        livre.deleteLivre(livre.LivreCollectionName, filter);

        // Try to find the deleted document
        results = livre.findLivres(livre.LivreCollectionName, filter);
        if (results.isEmpty()) {
            System.out.println("Document deleted successfully.");
        } else {
            System.out.println("Document was not deleted.");
        }
    }

}
