// Set MYPATH=C:\BDS2
// javac -g -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% %MYPATH%\MongoDB\Editeur.java
// java -Xmx256m -Xms256m -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% MongoDB.Editeur

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
import java.util.ArrayList;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;
import java.nio.file.Files;
import java.nio.file.Paths;



public class Editeur {
	private MongoDatabase database;	//instance d'une base, pointeur
   	private String dbName="Library";		//nom de la base
    	private String hostName="localhost";
    	private int port=27017;
    	private String userName="ulib";
    	private String passWord="passUlib";
    	private String collectionName="colEditeur";

	private String _id;		//id_Editeur
	private String nom;
	

	Editeur(){
		MongoClient mongoClient = new MongoClient( hostName , port ); 
		MongoCredential credential; 
		credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray()); 
		System.out.println("Connected to the database successfully"); 	  
		System.out.println("Credentials ::"+ credential);  
		database = mongoClient.getDatabase(dbName); 
	}

	
	public String get_id(){
		return _id;
	}

	public String getNom(){
		return nom;
	}

	public String getCollectionName(){
		return collectionName;
	}

	public void createCollection(String nomCollection){
    		database.createCollection(nomCollection); 
    		System.out.println("Collection "+ nomCollection + " created successfully"); 
	}

	public void readCollection(String nomCollection, Document whereQuery, Document sortFields, Document projectionFields){	
		MongoCollection<Document> collection = database.getCollection(nomCollection);
		System.out.println("\n\n\n*********** " + nomCollection +" *****************");
		FindIterable<Document> listDoc=collection.find(whereQuery).sort(sortFields).projection(projectionFields);
		// Getting the iterator
		Iterator it = listDoc.iterator();
		while(it.hasNext()) { System.out.println(it.next()); }
	}


	public void updateCollection(String nomCollection, Document whereQuery, Document updateExpressions, UpdateOptions updateOptions){
		MongoCollection<Document> collection = database.getCollection(nomCollection);
		UpdateResult updateResult = collection.updateMany(whereQuery, updateExpressions);
		System.out.println("\n\n\n*********** Dans "+ nomCollection +" *****************");
		System.out.println("\nRésultat update : "
		+"getUpdate id: "+updateResult
		+" getMatchedCount : "+updateResult.getMatchedCount()
		+" getModifiedCount : "+updateResult.getModifiedCount()
		);
	}

	public void deleteCollection(String nomCollection){
		MongoCollection<Document> collection = database.getCollection(nomCollection);
		if (collection==null)
        		System.out.println("Collection inexistante");
    		else {
        		collection.drop();	
        		System.out.println("Collection " + nomCollection + " deleted successfully"); 
    		}
	}


	public void insertOneDocument(String nomCollection, Document document){
		MongoCollection<Document> collection = database.getCollection(nomCollection);
		collection.insertOne(document); 
		System.out.println("Document inserted successfully");     
   	}

   	public void insertManyDocuments(String nomCollection, List<Document> documents){
    		MongoCollection<Document> collection = database.getCollection(nomCollection);
    		collection.insertMany(documents); 
    		System.out.println("Many Documents inserted successfully");     
	}

	public void deleteManyDocuments(String nomCollection, Document filters){
		System.out.println("\n\n\n*********** Dans "+ nomCollection +" *****************");
		MongoCollection<Document> collection = database.getCollection(nomCollection);
		collection.deleteMany(filters);
	}

	
	//Written by Hadil and Milena
	public void insertJsonData(String collectionName, String jsonFileName) {
    		String jsonFilePath = Paths.get(System.getenv("MYPATH"),  "data", jsonFileName).toString();
    		try {
        		String content = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
        		List<Document> documents = new ArrayList<>();

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

               			Document doc = Document.parse(jsonObject);
				documents.add(doc);
            		}
        	}

        	insertManyDocuments(collectionName, documents);

    	} catch (Exception e) {
        	e.printStackTrace();
	    }
	}


	public static void main(String[] args) {
		Editeur editeur = new Editeur();
		editeur.insertJsonData(editeur.collectionName,"editeur.json");
		editeur.readCollection(editeur.collectionName, new Document(), new Document("_id",1), new Document()); 
	}
}
