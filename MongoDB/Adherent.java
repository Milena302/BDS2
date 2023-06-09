// Set MYPATH=C:\BDS2
// javac -g -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% %MYPATH%\MongoDB\Adherent.java
// java -Xmx256m -Xms256m -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% MongoDB.Adherent

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

public class Adherent{
    private MongoDatabase database;	//instance d'une base, pointeur
    private String dbName="Library";		//nom de la base
    private String hostName="localhost";
    private int port=27017;
    private String userName="ulib";
    private String passWord="passUlib";
    private String AdhCollectionName="colAdh";

    /**
     * Constructeur
     */

    Adherent(){		
        MongoClient mongoClient = new MongoClient( hostName , port ); 
        MongoCredential credential; 
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray()); 
        System.out.println("Connected to the database successfully"); 	  
        System.out.println("Credentials ::"+ credential);  
        database = mongoClient.getDatabase(dbName); 
    }

    /**
    * Methodes CRUD
    */
    public void createCollectionAdh(String nomCollection){
        database.createCollection(nomCollection); 
        System.out.println("Collection Adhs created successfully"); 
    }


    public void dropCollectionAdh(String nomCollection){
        //Drop a collection 
        MongoCollection<Document> colAdhs=null; 
        System.out.println("\n\n\n*********** dans dropCollectionEmp *****************");   
        System.out.println("!!!! Collection Adh : " + colAdhs);
        colAdhs=database.getCollection(nomCollection);
        System.out.println("!!!! Collection Adh : "+colAdhs);
        // Visiblement jamais !!!
        if (colAdhs==null)
            System.out.println("Collection inexistante");
        else {
            colAdhs.drop();	
            System.out.println("Collection colAdhs removed successfully !!!"); 
        }
    }


    public void insertOneAdh(String nomCollection, Document adh){
		//Drop a collection 
		MongoCollection<Document> colAdhs=database.getCollection(nomCollection);
		colAdhs.insertOne(adh); 
		System.out.println("Adh : Document inserted successfully");     
   }


    public void insertManyAdhs(String nomCollection, List<Document> adhs){
        //Drop a collection 
        MongoCollection<Document> colAdhs=database.getCollection(nomCollection);
        colAdhs.insertMany(adhs); 
        System.out.println("Adh : Many Documents inserted successfully");     
    }


    public void updateAdhs(String nomCollection, 
    Document whereQuery, 
    Document updateExpressions,
    UpdateOptions updateOptions
    ){
        System.out.println("\n\n\n*********** dans updateAdhs *****************");   

        MongoCollection<Document> colAdhs=database.getCollection(nomCollection);
        UpdateResult updateResult = colAdhs.updateMany(whereQuery, updateExpressions);
        
        System.out.println("\nResultat update : "
        +"getUpdate id: "+updateResult
        +" getMatchedCount : "+updateResult.getMatchedCount() 
        +" getModifiedCount : "+updateResult.getModifiedCount()
        );
    }


    public void deleteAdhs(String nomCollection, Document filters){
		
        System.out.println("\n\n\n*********** dans deleteAdhs *****************");   
        FindIterable<Document> listAdhs;
        Iterator it;
        MongoCollection<Document> colAdhs=database.getCollection(nomCollection);
        
        listAdhs=colAdhs.find(filters).sort(new Document("_id", 1));
        it = listAdhs.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteAdhs : avant suppression");
    
        colAdhs.deleteMany(filters);
        listAdhs=colAdhs.find(filters).sort(new Document("_id", 1));
        it = listAdhs.iterator();// Getting the iterator
        this.displayIterator(it, "Dans deleteAdhs : Apres suppression");
    }

    public void displayIterator(Iterator it, String message){
        System.out.println(" \n #### "+ message + " ################################");
        while(it.hasNext()) {
            System.out.println(it.next());
        }		
       }


       public void insertJsonData(String collectionName, String jsonFileName) {
        String jsonFilePath = Paths.get(System.getenv("MYPATH"),  "data", jsonFileName).toString();
        try {
            String content = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
            List<Document> adhDocuments = new ArrayList<>();

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

                    Document adhDoc = Document.parse(jsonObject);
                    adhDocuments.add(adhDoc);
                }
            }

            insertManyAdhs(collectionName, adhDocuments);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Document> findAdhs(String collectionName, Document filter) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        FindIterable<Document> result;
        if (filter != null) {
            result = collection.find(filter);
        } else {
            result = collection.find();
        }
        List<Document> adhs = new ArrayList<>();
        for (Document adh : result) {
            adhs.add(adh);
        }
        return adhs;
    }
    

    public void printAllAdhs(String collectionName) {
        List<Document> adhs = findAdhs(collectionName, null);
        for (Document adh : adhs) {
            System.out.println(adh.toJson());
        }
    } 

    public static void main(String[] args) {
        System.out.println("DEBUT DE ADHERENT");
        System.out.println("///////////////////////////////////////");
        Adherent adh = new Adherent();
        System.out.println("///////////////////////////////////////");
        adh.dropCollectionAdh(adh.AdhCollectionName);
        System.out.println("///////////////////////////////////////");
        adh.createCollectionAdh(adh.AdhCollectionName);
        System.out.println("///////////////////////////////////////");
        adh.insertJsonData(adh.AdhCollectionName, "adherent.json");
        System.out.println("///////////////////////////////////////");
        adh.printAllAdhs(adh.AdhCollectionName);
    }
}