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
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Comparator;

import javax.swing.tree.ExpandVetoException;

public class Editeur {
    private MongoDatabase database; // instance d'une base, pointeur
    private String dbName = "Library"; // nom de la base
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ulib";
    private String passWord = "passUlib";
    String EditeurCollectionName = "colEdit";

    Editeur() {
        MongoClient mongoClient = new MongoClient(hostName, port);
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("Connected to the database successfully");
        System.out.println("Credentials ::" + credential);
        database = mongoClient.getDatabase(dbName);
    }

    public String getEditName() {
        return this.EditeurCollectionName;
    }

    public void createCollectionEdit(String nomCollection) {
        database.createCollection(nomCollection);
        System.out.println("Collection Editeur created successfully");
    }

    public void dropCollectionEdit(String nomCollection) {
        // Drop a collection
        MongoCollection<Document> colEdit = null;
        System.out.println("\n\n\n*********** dans dropCollectionDept *****************");
        System.out.println("!!!! Collection Editeur : " + colEdit);
        colEdit = database.getCollection(nomCollection);
        System.out.println("!!!! Collection Editeur : " + colEdit);
        // Visiblement jamais !!!
        if (colEdit == null)
            System.out.println("Collection inexistante");
        else {
            colEdit.drop();
            System.out.println("Collection colEDIT removed successfully !!!");
        }
    }

    public void insertOneEdit(String nomCollection, Document edit) {
        // Drop a collection
        MongoCollection<Document> colEdit = database.getCollection(nomCollection);
        colEdit.insertOne(edit);
        System.out.println("Document inserted successfully");
    }

    public void insertManyEdits(String nomCollection, List<Document> edits) {
        // Drop a collection
        MongoCollection<Document> colEdit = database.getCollection(nomCollection);
        colEdit.insertMany(edits);
        System.out.println("Many Documents inserted successfully");
    }

    public void updateOneEdit(String nomCollection, Document edit) {
        // Drop a collection
        MongoCollection<Document> colEdit = database.getCollection(nomCollection);
        colEdit.insertOne(edit);
        System.out.println("Document inserted successfully");
    }

    public void updateManyEdit(String nomCollection, List<Document> edits) {
        // Drop a collection
        MongoCollection<Document> colEdit = database.getCollection(nomCollection);
        colEdit.insertMany(edits);
        System.out.println("Many Documents inserted successfully");
    }

    public void deleteOneEdit(String nomCollection, Document edit) {
        // Drop a collection
        MongoCollection<Document> colEdit = database.getCollection(nomCollection);
        colEdit.deleteOne(edit);
        System.out.println("Document inserted successfully");
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

            insertManyEdits(EditeurCollectionName, livreDocuments);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Document> findEditeurs() {
        MongoCollection<Document> colEdit = database.getCollection(EditeurCollectionName);
        return colEdit.find().into(new ArrayList<>());
    }

    public void printAllEditeurs() {
        List<Document> editeurs = findEditeurs();
        for (Document editeur : editeurs) {
            System.out.println(editeur.toJson());
        }
    }

    public void afficherEditeursPlusProductifs() {
        Livre livre = new Livre();
        List<Document> joinData = livre.joinEditeursWithLivres(EditeurCollectionName);

        Map<String, Integer> editeurCounts = new HashMap<>();
        for (Document document : joinData) {
            Object editeurObj = document.get("Editeur");
            String editeur = editeurObj != null ? editeurObj.toString() : "";
            editeurCounts.put(editeur, editeurCounts.getOrDefault(editeur, 0) + 1);
        }

        List<Map.Entry<String, Integer>> sortedEditeurs = new ArrayList<>(editeurCounts.entrySet());
        sortedEditeurs.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

        System.out.println("***************************************");
        System.out.println("Éditeurs les plus productifs :");
        System.out.println("***************************************");
        for (int i = 0; i < Math.min(5, sortedEditeurs.size()); i++) {
            Map.Entry<String, Integer> entry = sortedEditeurs.get(i);
            String editeur = entry.getKey();
            int count = entry.getValue();
            System.out.println("Éditeur : " + editeur + ", Nombre de livres : " + count);
        }
    }

    public void searchEditeurs(String collectionName, String nomEditeur) {
        MongoCollection<Document> colEditeurs = database.getCollection(collectionName);
        Document filter = new Document("Nom", nomEditeur);
        FindIterable<Document> iterable = colEditeurs.find(filter);

        Document editeur = iterable.first();

        if (editeur != null) {
            System.out.println(editeur.toJson());
        } else {
            System.out.println("L'éditeur '" + nomEditeur + "' n'existe pas.");
        }
    }

    public void createIndexOnNom() {
        MongoCollection<Document> colEdit = database.getCollection(EditeurCollectionName);
        colEdit.createIndex(new Document("Nom", 1));
    }

    public void createJointIndexOnNomAndPays() {
        MongoCollection<Document> colEdit = database.getCollection(EditeurCollectionName);
        colEdit.createIndex(new Document("Nom", 1).append("Pays", 1));
    }

    public static void main(String[] args) {
        Editeur editeur = new Editeur();
        // Livre livre = new Livre();

        editeur.createIndexOnNom();
        editeur.createJointIndexOnNomAndPays(); // joint index on 'Nom' and 'Pays' fields

        editeur.dropCollectionEdit(editeur.EditeurCollectionName);
        editeur.createCollectionEdit(editeur.EditeurCollectionName);
        editeur.insertJsonData(editeur.EditeurCollectionName, "editeur_data.json");

        editeur.afficherEditeursPlusProductifs();
        // editeur.afficherEditeursPlusProductifs3();
        editeur.findEditeurs();

        System.out.println("***************************************");
        System.out.println("rechereche d'un editeur par nom :");
        System.out.println("***************************************");
        String nomEditeur = "Pordal"; // Remplacez "Nom_de_l'éditeur" par le nom recherché

        editeur.searchEditeurs(editeur.EditeurCollectionName, nomEditeur);
    }
}
