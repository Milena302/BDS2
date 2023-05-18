// Set MYPATH=C:\BDS2
// javac -g -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% %MYPATH%\MongoDB\Employe.java
// java -Xmx256m -Xms256m -cp %MYPATH%\mongojar\mongo-java-driver-3.12.10.jar;%MYPATH% MongoDB.Employe

package MongoDB;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class Employe {

    public static void main(String[] args) {
        // Connexion à MongoDB
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            // Accéder à la base de données
            MongoDatabase database = mongoClient.getDatabase("ma_base_de_donnees");

            // Sélectionner la collection
            MongoCollection<Document> collection = database.getCollection("ma_collection");

            // Insérer un document
            Document document = new Document("nom", "John Doe")
                    .append("age", 30)
                    .append("ville", "Paris");

            collection.insertOne(document);
            System.out.println("Document inséré avec succès !");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
