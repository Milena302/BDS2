// Set MYPATH=C:\BDS2
// javac -g -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% %MYPATH%\MongoDB\Categorie.java
// java -Xmx256m -Xms256m -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% MongoDB.Categorie

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



public class Categorie {
	private MongoDatabase database;	//instance d'une base, pointeur
   	private String dbName="Library";		//nom de la base
    	private String hostName="localhost";
    	private int port=27017;
    	private String userName="ulib";
    	private String passWord="passUlib";
    	private String collectionName="colCategorie";

	private String _id;		//id_Categorie
	private String nom;
	

	Categorie(){
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

	public void createCollection(String nomCollection){
    		database.createCollection(nomCollection); 
    		System.out.println("Collection "+ nomCollection + " created successfully"); 
	}

	public void readCollection(String nomCollection, Document sortOptions){	
		MongoCollection<Document> collection = database.getCollection(nomCollection);
		System.out.println("\n\n\n*********** " + nomCollection +" *****************");
 		collection.find().sort(sortOptions);
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


	public static void main(String[] args) {
		Categorie categorie = new Categorie();
		categorie.readCollection("Categorie", new Document());
		categorie.readCollection(categorie.collectionName, new Document()); 

	}
}
