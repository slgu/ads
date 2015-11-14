package util;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import config.Config;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

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
}
