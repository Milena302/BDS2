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


public class Emprunt {
    private MongoDatabase database;	//instance d'une base, pointeur
    private String dbName="Library";		//nom de la base
    private String hostName="localhost";
    private int port=27017;
    private String userName="ulib";
    private String passWord="passUlib";
    private String EmpruntCollectionName="colEmprunt";


    Emprunt(){
        MongoClient mongoClient = new MongoClient( hostName , port );
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("Connected to the database successfully");
        System.out.println("Credentials ::"+ credential);
        database = mongoClient.getDatabase(dbName);
    }



    public void createCollectionEmprunt(String nomCollection){
        database.createCollection(nomCollection);
        System.out.println("Collection Emprunt created successfully");
    }


    public void dropCollectionEmprunt(String nomCollection){
        //Drop a collection
        MongoCollection<Document> colEmprunt=null;
        System.out.println("\n\n\n*********** dans dropCollectionDept *****************");
        System.out.println("!!!! Collection Emprunt : " + colEmprunt);
        colEmprunt=database.getCollection(nomCollection);
        System.out.println("!!!! Collection Emprunt : "+colEmprunt);
        // Visiblement jamais !!!
        if (colEmprunt==null)
            System.out.println("Collection inexistante");
        else {
            colEmprunt.drop();
            System.out.println("Collection colEmprunt removed successfully !!!");
        }
    }


    public void insertOneEmprunt(String nomCollection, Document emprunt){
        //Drop a collection
        MongoCollection<Document> colEmprunt=database.getCollection(nomCollection);
        colEmprunt.insertOne(emprunt);
        System.out.println("Document inserted successfully");
    }

    public void insertManyEmprunt(String nomCollection, List<Document> emprunts){
        //Drop a collection
        MongoCollection<Document> colEmprunt=database.getCollection(nomCollection);
        colEmprunt.insertMany(emprunts);
        System.out.println("Many Documents inserted successfully");
    }

    public void updateOneEmprunt(String nomCollection, Document emprunt){
        //Drop a collection
        MongoCollection<Document> colEmprunt=database.getCollection(nomCollection);
        colEmprunt.insertOne(emprunt);
        System.out.println("Document inserted successfully");
    }

    public void updateManyEmprunt(String nomCollection, List<Document> emprunts){
        //Drop a collection
        MongoCollection<Document> colEmprunt=database.getCollection(nomCollection);
        colEmprunt.insertMany(emprunts);
        System.out.println("Many Documents inserted successfully");
    }

    public void deleteOneEmprunt(String nomCollection, Document emprunt){
        //Drop a collection
        MongoCollection<Document> colEmprunt=database.getCollection(nomCollection);
        colEmprunt.deleteOne(emprunt);
        System.out.println("Document inserted successfully");
    }


    public void insertJsonData(String collectionName, String jsonFileName) {
        String jsonFilePath = Paths.get(System.getenv("MYPATH"),  "data", jsonFileName).toString();
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
            emprunts.add(emprunt);
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

    public static void main(String[] args) {
        System.out.println("DEBUT DE EMPRUNT");
        System.out.println("///////////////////////////////////////");
        Emprunt emprunt = new Emprunt();
        System.out.println("///////////////////////////////////////");
        emprunt.dropCollectionEmprunt(emprunt.EmpruntCollectionName);
        System.out.println("///////////////////////////////////////");
        emprunt.createCollectionEmprunt(emprunt.EmpruntCollectionName);
        System.out.println("///////////////////////////////////////");
        emprunt.insertJsonData(emprunt.EmpruntCollectionName, "emprunt.json");
        System.out.println("///////////////////////////////////////");
        emprunt.printAllEmprunts(emprunt.EmpruntCollectionName);

        List<Document> joinedData = emprunt.joinEmpruntWithAdherent(emprunt.EmpruntCollectionName);
        for (Document document : joinedData) {
            System.out.println(document.toJson());
        }
    }
}
