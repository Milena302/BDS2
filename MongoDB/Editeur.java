package MongoDB;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.List;

public class Editeur {
    private MongoDatabase database;	//instance d'une base, pointeur
    private String dbName="Library";		//nom de la base
    private String hostName="localhost";
    private int port=27017;
    private String userName="ulib";
    private String passWord="passUlib";
    private String EditeurCollectionName="colEdit";


    Editeur(){
        MongoClient mongoClient = new MongoClient( hostName , port );
        MongoCredential credential;
        credential = MongoCredential.createCredential(userName, dbName, passWord.toCharArray());
        System.out.println("Connected to the database successfully");
        System.out.println("Credentials ::"+ credential);
        database = mongoClient.getDatabase(dbName);
    }



    public void createCollectionEdit(String nomCollection){
        database.createCollection(nomCollection);
        System.out.println("Collection Editeur created successfully");
    }


    public void dropCollectionEdit(String nomCollection){
        //Drop a collection
        MongoCollection<Document> colEdit=null;
        System.out.println("\n\n\n*********** dans dropCollectionDept *****************");
        System.out.println("!!!! Collection Editeur : " + colEdit);
        colEdit=database.getCollection(nomCollection);
        System.out.println("!!!! Collection Editeur : "+colEdit);
        // Visiblement jamais !!!
        if (colEdit==null)
            System.out.println("Collection inexistante");
        else {
            colEdit.drop();
            System.out.println("Collection colEDIT removed successfully !!!");
        }
    }


    public void insertOneEdit(String nomCollection, Document edit){
        //Drop a collection
        MongoCollection<Document> colEdit=database.getCollection(nomCollection);
        colEdit.insertOne(edit);
        System.out.println("Document inserted successfully");
    }

    public void insertManyEdits(String nomCollection, List<Document> edits){
        //Drop a collection
        MongoCollection<Document> colEdit=database.getCollection(nomCollection);
        colEdit.insertMany(edits);
        System.out.println("Many Documents inserted successfully");
    }

    public void updateOneEdit(String nomCollection, Document edit){
        //Drop a collection
        MongoCollection<Document> colEdit=database.getCollection(nomCollection);
        colEdit.insertOne(edit);
        System.out.println("Document inserted successfully");
    }

    public void updateManyEdit(String nomCollection, List<Document> edits){
        //Drop a collection
        MongoCollection<Document> colEdit=database.getCollection(nomCollection);
        colEdit.insertMany(edits);
        System.out.println("Many Documents inserted successfully");
    }

    public void deleteOneEdit(String nomCollection, Document edit){
        //Drop a collection
        MongoCollection<Document> colEdit=database.getCollection(nomCollection);
        colEdit.deleteOne(edit);
        System.out.println("Document inserted successfully");
    }

    public static void main(String[] args) {
        Editeur editeur = new Editeur();
        editeur.dropCollectionEdit(editeur.EditeurCollectionName);
        editeur.createCollectionEdit(editeur.EditeurCollectionName);
    }
}
