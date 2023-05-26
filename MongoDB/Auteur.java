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

public class Auteur {
    private MongoDatabase database;
    private String dbName = "Library";
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ulib";
    private String passWord = "passUlib";
    private String auteurCollectionName = "colAuteurs";

    Auteur() {
        MongoClient mongoClient = new MongoClient(hostName, port);
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("Connected to the database successfully");
        System.out.println("Credentials ::" + credential);
        database = mongoClient.getDatabase(dbName);
    }

    public void createCollectionAuteurs(String collectionName) {
        database.createCollection(collectionName);
        System.out.println("Collection Auteurs created successfully");
    }

    public void dropCollectionAuteurs(String collectionName) {
        MongoCollection<Document> colAuteurs = database.getCollection(collectionName);
        if (colAuteurs != null) {
            colAuteurs.drop();
            System.out.println("Collection colAuteurs removed successfully");
        }
    }

    public void insertOneAuteur(String collectionName, Document auteur) {
        MongoCollection<Document> colAuteurs = database.getCollection(collectionName);
        colAuteurs.insertOne(auteur);
        System.out.println("Document inserted successfully");
    }

    public void insertManyAuteurs(String collectionName, List<Document> auteurs) {
        MongoCollection<Document> colAuteurs = database.getCollection(collectionName);
        colAuteurs.insertMany(auteurs);
        System.out.println("Many Documents inserted successfully");
    }

    public List<Document> findAuteurs(String collectionName, Document filter) {
        MongoCollection<Document> colAuteurs = database.getCollection(collectionName);
        FindIterable<Document> iterable = filter == null ? colAuteurs.find() : colAuteurs.find(filter);

        List<Document> results = new ArrayList<>();
        for (Document document : iterable) {
            results.add(document);
        }
        return results;
    }

    public void updateAuteur(String collectionName, Document filter, Document update) {
        MongoCollection<Document> colAuteurs = database.getCollection(collectionName);
        UpdateResult result = colAuteurs.updateOne(filter, new Document("$set", update));
        System.out.println("Number of documents updated : " + result.getModifiedCount());
    }

    public void deleteAuteur(String collectionName, Document filter) {
        MongoCollection<Document> colAuteurs = database.getCollection(collectionName);
        colAuteurs.deleteOne(filter);
        System.out.println("Document deleted successfully");
    }

    public static void main(String[] args) {
        Auteur auteur = new Auteur();
        auteur.dropCollectionAuteurs(auteur.auteurCollectionName);
        auteur.createCollectionAuteurs(auteur.auteurCollectionName);

        Document doc = new Document("name", "F. Scott Fitzgerald")
                .append("birthdate", "1896-09-24")
                .append("nationality", "American");

        auteur.insertOneAuteur(auteur.auteurCollectionName, doc);

        Document update = new Document("nationality", "USA");
        Document filter = new Document("name", "F. Scott Fitzgerald");
        auteur.updateAuteur(auteur.auteurCollectionName, filter, update);

        List<Document> results = auteur.findAuteurs(auteur.auteurCollectionName, filter);
        for (Document document : results) {
            System.out.println(document.toJson());
        }

        auteur.deleteAuteur(auteur.auteurCollectionName, filter);

        results = auteur.findAuteurs(auteur.auteurCollectionName, filter);
        if (results.isEmpty()) {
            System.out.println("Document deleted successfully.");
        } else {
            System.out.println("Document was not deleted.");
        }
    }
}
