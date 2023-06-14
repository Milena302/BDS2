// Set MYPATH=C:\BDS2
// javac -g -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% %MYPATH%\MongoDB\Employe.java
// java -Xmx256m -Xms256m -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% MongoDB.Employe

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

import com.mongodb.client.model.Sorts;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import org.bson.Document;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Employe {
    private MongoDatabase database; // instance d'une base, pointeur
    private String dbName = "Library"; // nom de la base
    private String hostName = "localhost";
    private int port = 27017;
    private String userName = "ulib";
    private String passWord = "passUlib";
    private String EmpCollectionName = "colEmp";

    public String getEmployeCollectionName() {
        return this.EmpCollectionName;
    }

    /**
     * Constructeur
     */

    Employe() {
        MongoClient mongoClient = new MongoClient(hostName, port);
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("Connected to the database successfully");
        System.out.println("Credentials ::" + credential);
        database = mongoClient.getDatabase(dbName);
    }

    /**
     * Methodes CRUD
     */

    public void createCollectionEmp(String nomCollection) {
        database.createCollection(nomCollection);
        System.out.println("Collection Emps created successfully");
    }

    public void dropCollectionEmp(String nomCollection) {
        // Drop a collection
        MongoCollection<Document> colEmps = null;
        System.out.println("\n\n\n*********** dans dropCollectionEmp *****************");
        System.out.println("!!!! Collection Emp : " + colEmps);
        colEmps = database.getCollection(nomCollection);
        System.out.println("!!!! Collection Emp : " + colEmps);
        // Visiblement jamais !!!
        if (colEmps == null)
            System.out.println("Collection inexistante");
        else {
            colEmps.drop();
            System.out.println("Collection colEmps removed successfully !!!");
        }
    }

    public void insertOneEmp(String nomCollection, Document emp) {
        // Drop a collection
        MongoCollection<Document> colEmps = database.getCollection(nomCollection);
        colEmps.insertOne(emp);
        System.out.println("Emp : Document inserted successfully");
    }

    public void insertManyEmps(String nomCollection, List<Document> emps) {
        // Drop a collection
        MongoCollection<Document> colEmps = database.getCollection(nomCollection);
        colEmps.insertMany(emps);
        System.out.println("Emp : Many Documents inserted successfully");
    }

    public void updateEmps(String nomCollection,
            Document whereQuery,
            Document updateExpressions,
            UpdateOptions updateOptions) {
        System.out.println("\n\n\n*********** dans updateEmps *****************");

        MongoCollection<Document> colEmps = database.getCollection(nomCollection);
        UpdateResult updateResult = colEmps.updateMany(whereQuery, updateExpressions);

        System.out.println("\nResultat update : "
                + "getUpdate id: " + updateResult
                + " getMatchedCount : " + updateResult.getMatchedCount()
                + " getModifiedCount : " + updateResult.getModifiedCount());
    }

    public void deleteEmps(String nomCollection, Document filters) {

        System.out.println("\n\n\n*********** dans deleteEmps *****************");
        FindIterable<Document> listEmp;
        Iterator it;
        MongoCollection<Document> colEmps = database.getCollection(nomCollection);

        listEmp = colEmps.find(filters).sort(new Document("_id", 1));
        it = listEmp.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteEmps : avant suppression");

        colEmps.deleteMany(filters);
        listEmp = colEmps.find(filters).sort(new Document("_id", 1));
        it = listEmp.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteEmps : Apres suppression");
    }

    public void displayIterator(Iterator it, String message) {
        System.out.println(" \n #### " + message + " ################################");
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }

    public void insertJsonData(String collectionName, String jsonFileName) {
        String jsonFilePath = Paths.get(System.getenv("MYPATH"), "data", jsonFileName).toString();
        try {
            String content = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
            List<Document> empDocuments = new ArrayList<>();

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

                    Document empDoc = Document.parse(jsonObject);

                    // Conversion de la date au format "MM/dd/yyyy" en objet LocalDate
                    String dateStr = empDoc.getString("age");
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
                    LocalDate date = LocalDate.parse(dateStr, formatter);
                    empDoc.put("age", Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant()));

                    empDocuments.add(empDoc);
                }
            }

            insertManyEmps(collectionName, empDocuments);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Document> findEmps(String collectionName, Document filter) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        FindIterable<Document> result;
        if (filter != null) {
            result = collection.find(filter);
        } else {
            result = collection.find();
        }
        List<Document> emps = new ArrayList<>();
        for (Document emp : result) {
            emps.add(emp);
        }
        return emps;
    }

    public void printAllEmps(String collectionName) {
        List<Document> emps = findEmps(collectionName, null);
        for (Document emp : emps) {
            System.out.println(emp.toJson());
        }
    }

    /*
     * Methodes d'affichage
     * 
     */

    // Si ascending = true alors ordre croissant
    // sinon ordre decroissant

    public void displayEmpsByField(String collectionName, String fieldName, boolean ascending) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        FindIterable<Document> result;

        if (ascending) {
            result = collection.find().sort(Sorts.ascending(fieldName));
            System.out.println("Documents sorted by field '" + fieldName + "' in ascending order:");
        } else {
            result = collection.find().sort(Sorts.descending(fieldName));
            System.out.println("Documents sorted by field '" + fieldName + "' in descending order:");
        }

        for (Document emp : result) {
            System.out.println(emp.toJson());
        }
    }

    public void getEmployes(String nomCollection,
            Document whereQuery,
            Document projectionFields,
            Document sortFields) {
        System.out.println("\n\n\n** dans getEmployes **");

        MongoCollection<Document> colEmps = database.getCollection(nomCollection);

        FindIterable<Document> listEmp = colEmps.find(whereQuery).sort(sortFields).projection(projectionFields);

        // Getting the iterator
        Iterator it = listEmp.iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }

    public static Date getDateFromString(String dateString) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
            return format.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void createIndexOnField(String collectionName, String fieldName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.createIndex(new Document(fieldName, 1));
        System.out.println("Index created on field: " + fieldName);
    }

    public void createIndexOnJoinField(String collectionName, String fieldName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.createIndex(new Document(fieldName, 1));
        System.out.println("Index created on join field: " + fieldName);
    }

    public static void main(String[] args) {
        System.out.println("DEBUT DE EMPLOYE");
        System.out.println("///////////////////////////////////////");
        Employe emp = new Employe();
        System.out.println("///////////////////////////////////////");
        emp.dropCollectionEmp(emp.EmpCollectionName);
        System.out.println("///////////////////////////////////////");
        emp.createCollectionEmp(emp.EmpCollectionName);
        System.out.println("///////////////////////////////////////");
        emp.insertJsonData(emp.EmpCollectionName, "employe.json");
        System.out.println("///////////////////////////////////////");
        // emp.printAllEmps(emp.EmpCollectionName);
        System.out.println("///////////////////////////////////////");
        // emp.displayEmpsByField(emp.EmpCollectionName, "nom", false);

        emp.createIndexOnField(emp.EmpCollectionName, "nom");
        emp.createIndexOnJoinField("colEmprunt", "date_emprunt");

        // Pour tester getEmployes
        // Pour tout afficher

        /*
         * emp.getEmployes(emp.EmpCollectionName,
         * new Document(),
         * new Document(),
         * new Document());
         * 
         * 
         * //Pour afficher en triant par ordre croissant sur le nom et par ordre
         * décroissant sur l'âge
         * emp.getEmployes(emp.EmpCollectionName,
         * new Document(),
         * new Document(),
         * new Document("nom", 1).append("age", -1));
         * 
         * //Pour afficher en projetant sur le nom :
         * emp.getEmployes(emp.EmpCollectionName,
         * new Document(),
         * new Document(new Document("nom", 1)),
         * new Document());
         * 
         * 
         * //Pour afficher en projetant sur le nom et en triant par ordre croissant :
         * emp.getEmployes(emp.EmpCollectionName,
         * new Document(),
         * new Document(new Document("nom", 1)),
         * new Document(new Document("nom", 1)));
         * 
         * 
         * //Pour trier par ordre croissant la date de naissance des employes :
         * Document whereQuery = new Document();
         * Document projectionFields = new Document();
         * Document sortFields = new Document("age", 1);
         * 
         * emp.getEmployes(emp.EmpCollectionName, whereQuery, projectionFields,
         * sortFields);
         * 
         */

        // Pour afficher les employes dont le nom commence par A :

        System.out.println("\n***************************************");
        System.out.println("Pour afficher les employes dont le nom commence par A");
        System.out.println("***************************************\n");

        emp.getEmployes(emp.EmpCollectionName,
                new Document("nom", new Document("$regex", "^A")),
                new Document(),
                new Document());

    }
}
