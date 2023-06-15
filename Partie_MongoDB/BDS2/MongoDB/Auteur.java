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

public class Auteur {
    private MongoDatabase database;
    private String dbName = "Library";
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ulib";
    private String passWord = "passUlib";
    private String auteurCollectionName = "colAuteurs";

    public String getAuteurCollectionName() {
        return this.auteurCollectionName;
    }

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

    public void insertJsonData(String collectionName, String jsonFileName) {
        String jsonFilePath = Paths.get(System.getenv("MYPATH"), "data", jsonFileName).toString();
        try {
            String content = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
            List<Document> authorDocuments = new ArrayList<>();

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

                    Document authorDoc = Document.parse(jsonObject);
                    authorDocuments.add(authorDoc);
                }
            }

            insertManyAuteurs(collectionName, authorDocuments);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void printAllAuteurs(String collectionName) {
        List<Document> auteurs = findAuteurs(collectionName, null);
        for (Document auteur : auteurs) {
            System.out.println(auteur.toJson());
        }
    }

    public Document findAuthorWithMostBooks(String livreCollectionName) {
        MongoCollection<Document> colLivres = database.getCollection(livreCollectionName);

        // Group by Auteur and count the number of books
        List<Document> pipeline = Arrays.asList(
                new Document("$group",
                        new Document("_id", "$Auteur")
                                .append("count", new Document("$sum", 1))),
                new Document("$sort", new Document("count", -1)),
                new Document("$limit", 1),
                new Document("$lookup",
                        new Document("from", auteurCollectionName)
                                .append("localField", "_id")
                                .append("foreignField", "_id")
                                .append("as", "author_info")),
                new Document("$unwind", "$author_info"),
                new Document("$project",
                        new Document("_id", 0)
                                .append("Author", "$author_info.Nom")
                                .append("BookCount", "$count")));

        List<Document> results = colLivres.aggregate(pipeline).into(new ArrayList<>());
        if (!results.isEmpty()) {
            return results.get(0);
        }

        return null;
    }

    public List<Document> findBookByYoungestAndOldestAuthor(String livreCollectionName) {
        MongoCollection<Document> colAuteurs = database.getCollection(auteurCollectionName);

        List<Document> pipeline = Arrays.asList(
                new Document("$addFields",
                        new Document("birthdate",
                                new Document("$dateFromString",
                                        new Document("dateString", "$date_naissance")))),
                new Document("$sort",
                        new Document("birthdate", -1)), // Sort by birthdate in descending order
                new Document("$lookup",
                        new Document("from", livreCollectionName)
                                .append("localField", "_id")
                                .append("foreignField", "Auteur")
                                .append("as", "books")),
                new Document("$unwind", "$books"),
                new Document("$project",
                        new Document("_id", 0)
                                .append("AuthorName", new Document("$concat", Arrays.asList("$Prenom", " ", "$Nom")))
                                .append("BookName", "$books.Titre")
                                .append("Birthdate", "$date_naissance")));

        List<Document> results = colAuteurs.aggregate(pipeline).into(new ArrayList<>());

        List<Document> booksByYoungestAndOldestAuthors = new ArrayList<>();
        if (!results.isEmpty()) {
            booksByYoungestAndOldestAuthors.add(results.get(0));
            Document oldestAuthorBook = results.stream()
                    .min((d1, d2) -> d1.getString("Birthdate").compareTo(d2.getString("Birthdate")))
                    .orElse(null);

            if (oldestAuthorBook != null) {
                booksByYoungestAndOldestAuthors.add(oldestAuthorBook);
            }
        }

        return booksByYoungestAndOldestAuthors;
    }

    public List<Document> findYoungestFrenchAuthors(int limit) {
        MongoCollection<Document> colAuteurs = database.getCollection(auteurCollectionName);

        // Create pipeline for aggregate operation
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("Pays", "France")),
                new Document("$addFields",
                        new Document("birthdate",
                                new Document("$dateFromString",
                                        new Document("dateString", "$date_naissance")))),
                new Document("$sort", new Document("birthdate", -1)),
                new Document("$limit", limit),
                new Document("$project",
                        new Document("_id", 0)
                                .append("AuthorName", new Document("$concat", Arrays.asList("$Prenom", " ", "$Nom")))
                                .append("Country", "$Pays")
                                .append("Birthdate", "$date_naissance")));

        List<Document> results = colAuteurs.aggregate(pipeline).into(new ArrayList<>());
        return results;
    }

    public void createIndexOnName(String collectionName) {
        MongoCollection<Document> colAuteurs = database.getCollection(collectionName);
        colAuteurs.createIndex(new Document("Nom", 1));
    }

    public void createComplexIndex(String collectionName) {
        MongoCollection<Document> colAuteurs = database.getCollection(collectionName);
        Document compoundIndex = new Document("Nom", 1)
                .append("Pays", 1)
                .append("date_naissance", -1);
        colAuteurs.createIndex(compoundIndex);
    }

    public static void main(String[] args) {
        Auteur auteur = new Auteur();
        auteur.dropCollectionAuteurs(auteur.auteurCollectionName);
        auteur.createCollectionAuteurs(auteur.auteurCollectionName);

        auteur.createIndexOnName(auteur.getAuteurCollectionName());
        auteur.createComplexIndex(auteur.getAuteurCollectionName());

        auteur.insertJsonData(auteur.auteurCollectionName, "auteur_data.json");

        auteur.printAllAuteurs(auteur.auteurCollectionName);

        Document authorWithMostBooks = auteur.findAuthorWithMostBooks("colLivres");
        if (authorWithMostBooks != null) {
            System.out.println("***************************************");
            System.out.println("Filtering by author who has the most books in our database  ");
            System.out.println("***************************************\n");
            System.out.println("Author with the most books:");
            System.out.println(authorWithMostBooks.toJson());
        } else {
            System.out.println("No authors found.");
        }

        List<Document> booksByYoungestAndOldestAuthors = auteur.findBookByYoungestAndOldestAuthor("colLivres");
        System.out.println("\n***************************************");
        System.out.println("Filtering by youngest and oldest author who has a book in our database");
        System.out.println("***************************************\n");
        for (Document book : booksByYoungestAndOldestAuthors) {
            System.out.println(book.toJson());
        }
        System.out.println("\n***************************************");
        System.out.println("Filtering by youngest 5 french authors");
        System.out.println("***************************************\n");

        List<Document> youngestFrenchAuthors = auteur.findYoungestFrenchAuthors(5);
        for (Document author : youngestFrenchAuthors) {
            System.out.println(author.toJson());
        }
    }

}