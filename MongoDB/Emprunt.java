package MongoDB;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

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

    public static void main(String[] args) {
        Emprunt emprunt = new Emprunt();
        emprunt.dropCollectionEmprunt(emprunt.EmpruntCollectionName);
        emprunt.createCollectionEmprunt(emprunt.EmpruntCollectionName);
    }
}
