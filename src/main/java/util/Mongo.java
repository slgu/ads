package util;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.gridfs.GridFS;
import config.Config;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static java.util.Arrays.asList;

/**
 * Created by slgu1 on 11/14/15.
 */
public class Mongo {
    public static MongoClient mongoclient = null;
    public static MongoDatabase mongodb = null;
    static {
        try {
            mongoclient = new MongoClient(new ServerAddress(InetAddress.getByName(Config.MongoIp), Config.MongoPort));
            mongodb = mongoclient.getDatabase(Config.MongoDb);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void sizeStatistic() {
        FindIterable <Document> res = mongodb.getCollection(Config.BlockConnection).find(
                new Document("referCnt", new Document("$gt", 1))
        );
        double sum = 0;
        for (Document doc: res) {
            sum += (Double)doc.get("size");
        }
        System.out.println(sum);
    }

    public static void main(String [] args) {
        sizeStatistic();
    }
}