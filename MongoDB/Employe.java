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



public class Employe {
    private MongoDatabase database;	//instance d'une base, pointeur
    private String dbName="Library";		//nom de la base
    private String hostName="localhost";
    private int port=27017;
    private String userName="ulib";
    private String passWord="passUlib";
    private String EmpCollectionName="colEmp";


    Employe(){		
		MongoClient mongoClient = new MongoClient( hostName , port ); 
		MongoCredential credential; 
		credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray()); 
		System.out.println("Connected to the database successfully"); 	  
		System.out.println("Credentials ::"+ credential);  
		database = mongoClient.getDatabase(dbName); 
   }



   public void createCollectionEmp(String nomCollection){
    database.createCollection(nomCollection); 
    System.out.println("Collection Depts created successfully"); 
}


public void dropCollectionEmp(String nomCollection){
    //Drop a collection 
    MongoCollection<Document> colEmps=null; 
    System.out.println("\n\n\n*********** dans dropCollectionDept *****************");   
    System.out.println("!!!! Collection Dept : " + colEmps);
    colEmps=database.getCollection(nomCollection);
    System.out.println("!!!! Collection Dept : "+colEmps);
    // Visiblement jamais !!!
    if (colEmps==null)
        System.out.println("Collection inexistante");
    else {
        colEmps.drop();	
        System.out.println("Collection colDepts removed successfully !!!"); 
    }
}


   public void insertOneEmp(String nomCollection, Document emp){
		//Drop a collection 
		MongoCollection<Document> colDepts=database.getCollection(nomCollection);
		colDepts.insertOne(emp); 
		System.out.println("Document inserted successfully");     
   }

   public void insertManyDepts(String nomCollection, List<Document> emps){
    //Drop a collection 
    MongoCollection<Document> colEmps=database.getCollection(nomCollection);
    colEmps.insertMany(emps); 
    System.out.println("Many Documents inserted successfully");     
}


    public static void main(String[] args) {
        Employe emp = new Employe();
        emp.dropCollectionEmp(emp.EmpCollectionName);
        emp.createCollectionEmp(emp.EmpCollectionName);
    }
}
