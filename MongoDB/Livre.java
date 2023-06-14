package MongoDB;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.bson.Document;
import com.mongodb.client.FindIterable;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.Arrays;

public class Livre {
    private MongoDatabase database;
    private String dbName = "Library";
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ulib";
    private String passWord = "passUlib";
    private String livreCollectionName = "colLivres";
    private String categorieCollectionName = "colCategories";

    public String getLivreCollectionName() {
        return this.livreCollectionName;
    }

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
        database.getCollection(collectionName).drop();
        System.out.println("Collection " + collectionName + " removed successfully");
    }

    public void insertOneLivre(String collectionName, Document livre) {
        database.getCollection(collectionName).insertOne(livre);
        System.out.println("Document inserted successfully");
    }

    public void insertManyLivres(String collectionName, List<Document> livres) {
        database.getCollection(collectionName).insertMany(livres);
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

    public List<Document> joinLivresWithCategories(String categorieCollectionName) {
        MongoCollection<Document> colLivres = database.getCollection(livreCollectionName);

        List<Document> pipeline = Arrays.asList(
                new Document("$lookup",
                        new Document("from", categorieCollectionName)
                                .append("localField", "Categorie")
                                .append("foreignField", "_id")
                                .append("as", "categorie_info")));

        List<Document> results = new ArrayList<>();
        for (Document document : colLivres.aggregate(pipeline)) {
            results.add(document);
        }
        return results;
    }

    public List<Document> findLivresByCategorie(String categorieCollectionName, int categorieId) {
        Document filter = new Document("Categorie", categorieId);
        List<Document> joinedData = joinLivresWithCategories(categorieCollectionName);

        List<Document> filteredLivres = new ArrayList<>();

        for (Document document : joinedData) {
            List<Document> categorieInfoList = document.getList("categorie_info", Document.class);
            for (Document categorieInfo : categorieInfoList) {
                if (categorieInfo.getInteger("_id") == categorieId) {
                    Document newDocument = new Document();
                    newDocument.put("_id", document.getInteger("_id"));
                    newDocument.put("Titre", document.getString("Titre"));
                    newDocument.put("Auteur", document.getInteger("Auteur"));
                    filteredLivres.add(newDocument);
                    break;
                }
            }
        }

        return filteredLivres;
    }

    public void insertJsonData(String collectionName, String jsonFileName) {
        String jsonFilePath = Paths.get(System.getenv("MYPATH"), "data", jsonFileName).toString();
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

            insertManyLivres(collectionName, livreDocuments);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int countLivresByCategorie(String categorieCollectionName, int categorieId) {
        Document filter = new Document("Categorie", categorieId);
        List<Document> joinedData = joinLivresWithCategories(categorieCollectionName);

        int count = 0;

        for (Document document : joinedData) {
            List<Document> categorieInfoList = document.getList("categorie_info", Document.class);
            for (Document categorieInfo : categorieInfoList) {
                if (categorieInfo.getInteger("_id") == categorieId) {
                    count++;
                    break;
                }
            }
        }

        return count;
    }

    public List<Document> joinEditeursWithLivres(String editCol) {

        MongoCollection<Document> colLivres = database.getCollection(livreCollectionName);

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

    public void createIndexOnId() {
        MongoCollection<Document> colLivres = database.getCollection(livreCollectionName);
        colLivres.createIndex(new Document("_id", 1));
    }

    public void createIndexOnCategorie() {
        MongoCollection<Document> colLivres = database.getCollection(livreCollectionName);
        colLivres.createIndex(new Document("Categorie", 1));
    }

    public static void main(String[] args) {
        Livre livre = new Livre();
        Editeur editeur = new Editeur();

        Categorie categorie = new Categorie();

        livre.dropCollectionLivres(livre.livreCollectionName);
        livre.createCollectionLivres(livre.livreCollectionName);

        livre.createIndexOnId();
        livre.createIndexOnCategorie();

        livre.insertJsonData(livre.livreCollectionName, "livre_data.json");

        System.out.println("***************************************");
        System.out.println("Filtering by categories");
        System.out.println("***************************************");

        int categorieId = 1; // specify the category ID you are interested in here

        List<Document> livresByCategorie = livre.findLivresByCategorie(categorie.getCategorieCollectionName(),
                categorieId);
        System.out.println("\n Livres from Categorie ID " + categorieId + ":\n");
        for (Document livreDoc : livresByCategorie) {
            System.out.println(livreDoc.toJson());
        }

        int count = livre.countLivresByCategorie(categorie.getCategorieCollectionName(), categorieId);
        System.out.println("\n Number of Livres from Categorie ID " + categorieId + ": " + count + "\n");

        List<Document> result = livre.joinEditeursWithLivres(editeur.getEditName());
        for (Document document : result) {
            // System.out.println(document.toJson());
        }
    }

}
