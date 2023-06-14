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

import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;

public class Emprunt {
    private MongoDatabase database; // instance d'une base, pointeur
    private String dbName = "Library"; // nom de la base
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ulib";
    private String passWord = "passUlib";
    private String EmpruntCollectionName = "colEmprunt";

    Emprunt() {
        MongoClient mongoClient = new MongoClient(hostName, port);
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("Connected to the database successfully");
        System.out.println("Credentials ::" + credential);
        database = mongoClient.getDatabase(dbName);
    }

    public void createCollectionEmprunt(String nomCollection) {
        database.createCollection(nomCollection);
        System.out.println("Collection Emprunt created successfully");
    }

    public void dropCollectionEmprunt(String nomCollection) {
        // Drop a collection
        MongoCollection<Document> colEmprunt = null;
        System.out.println("\n\n\n*********** dans dropCollectionDept *****************");
        System.out.println("!!!! Collection Emprunt : " + colEmprunt);
        colEmprunt = database.getCollection(nomCollection);
        System.out.println("!!!! Collection Emprunt : " + colEmprunt);
        // Visiblement jamais !!!
        if (colEmprunt == null)
            System.out.println("Collection inexistante");
        else {
            colEmprunt.drop();
            System.out.println("Collection colEmprunt removed successfully !!!");
        }
    }

    public void insertOneEmprunt(String nomCollection, Document emprunt) {
        // Drop a collection
        MongoCollection<Document> colEmprunt = database.getCollection(nomCollection);
        colEmprunt.insertOne(emprunt);
        System.out.println("Document inserted successfully");
    }

    public void insertManyEmprunt(String nomCollection, List<Document> emprunts) {
        // Drop a collection
        MongoCollection<Document> colEmprunt = database.getCollection(nomCollection);
        colEmprunt.insertMany(emprunts);
        System.out.println("Many Documents inserted successfully");
    }

    public void updateOneEmprunt(String nomCollection, Document emprunt) {
        // Drop a collection
        MongoCollection<Document> colEmprunt = database.getCollection(nomCollection);
        colEmprunt.insertOne(emprunt);
        System.out.println("Document inserted successfully");
    }

    public void updateManyEmprunt(String nomCollection, List<Document> emprunts) {
        // Drop a collection
        MongoCollection<Document> colEmprunt = database.getCollection(nomCollection);
        colEmprunt.insertMany(emprunts);
        System.out.println("Many Documents inserted successfully");
    }

    public void deleteOneEmprunt(String nomCollection, Document emprunt) {
        // Drop a collection
        MongoCollection<Document> colEmprunt = database.getCollection(nomCollection);
        colEmprunt.deleteOne(emprunt);
        System.out.println("Document inserted successfully");
    }

    public void insertJsonData(String collectionName, String jsonFileName) {
        String jsonFilePath = Paths.get(System.getenv("MYPATH"), "data", jsonFileName).toString();
        try {
            String content = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
            List<Document> empruntDocuments = new ArrayList<>();

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

                    Document empruntDoc = Document.parse(jsonObject);
                    empruntDocuments.add(empruntDoc);
                }
            }

            insertManyEmprunt(collectionName, empruntDocuments);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Document> findEmprunts(String collectionName, Document filter) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        FindIterable<Document> result;
        if (filter != null) {
            result = collection.find(filter);
        } else {
            result = collection.find();
        }
        List<Document> emprunts = new ArrayList<>();
        for (Document emprunt : result) {
            // emprunts.add(emprunt);
        }
        return emprunts;
    }

    public void printAllEmprunts(String collectionName) {
        List<Document> emprunts = findEmprunts(collectionName, null);
        for (Document emprunt : emprunts) {
            System.out.println(emprunt.toJson());
        }
    }

    public List<Document> joinEmpruntWithAdherent(String adherentCollectionName) {
        MongoCollection<Document> colEmp = database.getCollection(EmpruntCollectionName); // fixed variable name

        List<Document> pipeline = Arrays.asList(
                new Document("$lookup",
                        new Document("from", adherentCollectionName)
                                .append("localField", "id_adherent")
                                .append("foreignField", "_id")
                                .append("as", "adherent_info")));

        List<Document> results = new ArrayList<>();
        for (Document document : colEmp.aggregate(pipeline)) {
            results.add(document);
        }
        return results;
    }

    public List<Document> joinEmpruntWithEmploye(String adherentCollectionName) {
        MongoCollection<Document> colEmp = database.getCollection(EmpruntCollectionName); // fixed variable name

        List<Document> pipeline = Arrays.asList(
                new Document("$lookup",
                        new Document("from", adherentCollectionName)
                                .append("localField", "id_employe")
                                .append("foreignField", "_id")
                                .append("as", "employe_info")));

        List<Document> results = new ArrayList<>();
        for (Document document : colEmp.aggregate(pipeline)) {
            results.add(document);
        }
        return results;
    }

    public void printEmpruntsByAdherentId(String collectionName, int adherentId) {
        Document filter = new Document("id_adherent", adherentId);
        List<Document> emprunts = findEmprunts(collectionName, filter);
        System.out.println("Emprunts effectues par Adherent dont l'id est " + adherentId);
        for (Document emprunt : emprunts) {
            System.out.println(emprunt.toJson());
        }
    }

    public int getAdherentWithMostEmprunts(String collectionName) {
        List<Document> emprunts = findEmprunts(collectionName, null);

        // Compter le nombre d'emprunts par adhérent
        Map<Integer, Integer> empruntsByAdherent = new HashMap<>();
        for (Document emprunt : emprunts) {
            int adherentId = emprunt.getInteger("id_adherent");
            empruntsByAdherent.put(adherentId, empruntsByAdherent.getOrDefault(adherentId, 0) + 1);
        }

        // Trouver l'adhérent avec le nombre maximum d'emprunts
        int maxEmprunts = 0;
        int adherentIdWithMostEmprunts = -1;
        for (Map.Entry<Integer, Integer> entry : empruntsByAdherent.entrySet()) {
            int adherentId = entry.getKey();
            int numEmprunts = entry.getValue();
            if (numEmprunts > maxEmprunts) {
                maxEmprunts = numEmprunts;
                adherentIdWithMostEmprunts = adherentId;
            }
        }

        return adherentIdWithMostEmprunts;
    }

    public List<Document> joinEmpruntWithLivre(String livreCollectionName) {
        MongoCollection<Document> colEmp = database.getCollection(EmpruntCollectionName);

        List<Document> pipeline = Arrays.asList(
                new Document("$lookup",
                        new Document("from", livreCollectionName)
                                .append("localField", "id_livre")
                                .append("foreignField", "_id")
                                .append("as", "livre_info")));

        List<Document> results = new ArrayList<>();
        for (Document document : colEmp.aggregate(pipeline)) {
            results.add(document);
        }
        return results;
    }

    public List<Document> getTop5Books(String livreCollectionName) {
        MongoCollection<Document> colEmp = database.getCollection(EmpruntCollectionName);

        List<Document> pipeline = Arrays.asList(
                new Document("$group", new Document("_id", "$id_livre").append("count", new Document("$sum", 1))),
                new Document("$sort", new Document("count", -1)),
                new Document("$limit", 5));

        List<Document> results = new ArrayList<>();
        for (Document document : colEmp.aggregate(pipeline)) {
            results.add(document);
        }

        MongoCollection<Document> colLiv = database.getCollection(livreCollectionName);
        for (Document result : results) {
            Document livre = colLiv.find(new Document("_id", result.getInteger("_id"))).first();
            System.out.println(
                    "Book Name: " + livre.getString("Titre") + ", Number of Emprunts: " + result.getInteger("count"));
        }

        return results;
    }

    public Document getTopEmployee(String employeCollectionName) {
        MongoCollection<Document> colEmp = database.getCollection(EmpruntCollectionName);

        List<Document> pipeline = Arrays.asList(
                new Document("$group", new Document("_id", "$id_employe").append("count", new Document("$sum", 1))),
                new Document("$sort", new Document("count", -1)),
                new Document("$limit", 1));

        Document topEmployeeEmprunt = colEmp.aggregate(pipeline).first();
        MongoCollection<Document> colEmpl = database.getCollection(employeCollectionName);
        Document topEmployee = colEmpl.find(new Document("_id", topEmployeeEmprunt.getInteger("_id"))).first();

        System.out.println("Employee Name: " + topEmployee.getString("nom") + ", Number of Emprunts: "
                + topEmployeeEmprunt.getInteger("count"));

        return topEmployee;
    }

    public Document getTopAdherent(String adherentCollectionName) {
        MongoCollection<Document> colEmp = database.getCollection(EmpruntCollectionName);

        List<Document> pipeline = Arrays.asList(
                new Document("$group", new Document("_id", "$id_adherent").append("count", new Document("$sum", 1))),
                new Document("$sort", new Document("count", -1)),
                new Document("$limit", 1));

        Document topAdherentEmprunt = colEmp.aggregate(pipeline).first();
        MongoCollection<Document> colAdh = database.getCollection(adherentCollectionName);
        Document topAdherent = colAdh.find(new Document("_id", topAdherentEmprunt.getInteger("_id"))).first();

        System.out.println("Adherent Name: " + topAdherent.getString("prenom") + " " + topAdherent.getString("nom")
                + ", Gender: " + topAdherent.getString("genre")
                + ", Email: " + topAdherent.getString("email")
                + ", Phone Number: " + topAdherent.getString("telephone")
                + ", Address: " + topAdherent.getString("adresse")
                + ", Number of Emprunts: " + topAdherentEmprunt.getInteger("count"));

        return topAdherent;
    }

    public void createIndexOnAdherentId() {
        MongoCollection<Document> colEmprunt = database.getCollection(EmpruntCollectionName);
        colEmprunt.createIndex(new Document("id_adherent", 1));
    }

    public void createJointIndexOnAdherentIdAndEmployeId() {
        MongoCollection<Document> colEmprunt = database.getCollection(EmpruntCollectionName);
        colEmprunt.createIndex(new Document("id_adherent", 1).append("id_employe", 1));
    }

    public static void main(String[] args) {
        Livre livre = new Livre();
        Employe emp = new Employe();
        Adherent adherent = new Adherent();

        Emprunt emprunt = new Emprunt();

        emprunt.createIndexOnAdherentId();
        emprunt.createJointIndexOnAdherentIdAndEmployeId();

        System.out.println("DEBUT DE EMPRUNT");
        System.out.println("///////////////////////////////////////");
        System.out.println("///////////////////////////////////////");
        emprunt.dropCollectionEmprunt(emprunt.EmpruntCollectionName);
        System.out.println("///////////////////////////////////////");
        emprunt.createCollectionEmprunt(emprunt.EmpruntCollectionName);
        System.out.println("///////////////////////////////////////");
        emprunt.insertJsonData(emprunt.EmpruntCollectionName, "emprunt.json");
        System.out.println("///////////////////////////////////////");
        // emprunt.printAllEmprunts(emprunt.EmpruntCollectionName);

        List<Document> joinedDataAdh = emprunt.joinEmpruntWithAdherent(emprunt.EmpruntCollectionName);
        for (Document document : joinedDataAdh) {
            // System.out.println(document.toJson());
        }

        List<Document> joinedDataEmp = emprunt.joinEmpruntWithEmploye(emprunt.EmpruntCollectionName);
        for (Document document : joinedDataEmp) {
            // System.out.println(document.toJson());
        }

        List<Document> joinedDataLivre = emprunt.joinEmpruntWithLivre(livre.getLivreCollectionName());
        for (Document document : joinedDataLivre) {
            // System.out.println(document.toJson());
        }

        emprunt.printEmpruntsByAdherentId(emprunt.EmpruntCollectionName, 42);

        int adherentIdWithMostEmprunts = emprunt.getAdherentWithMostEmprunts(emprunt.EmpruntCollectionName);
        System.out.println("ID de l'adhérent avec le plus d'emprunts : " + adherentIdWithMostEmprunts);

        System.out.println("\n***************************************");
        System.out.println("les 5 livres les plus empruntés");
        System.out.println("***************************************\n");
        emprunt.getTop5Books(livre.getLivreCollectionName());

        System.out.println("\n***************************************");
        System.out.println("l'employé qui a fait le plus d'emprunts");
        System.out.println("***************************************\n");
        emprunt.getTopEmployee(emp.getEmployeCollectionName());
        System.out.println("\n***************************************");
        System.out.println("l'adherant avec le plus d'emprunts");
        System.out.println("***************************************\n");
        emprunt.getTopAdherent(adherent.getAdherentCollectionName());
    }
}
