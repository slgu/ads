package operation;

import com.mongodb.client.FindIterable;
import config.Config;
import dedup.Pair;
import org.bson.Document;
import util.Hdfs;
import util.Mongo;
import util.Util;

import javax.print.Doc;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by slgu1 on 11/13/15.
 */
public class FileOp {
    public static boolean create(String filename, InputStream io) {
        //handler
        String uuid = Util.uuid();
        try {
            Mongo.mongodb.getCollection(Config.FileConnection).insertOne(
                    new Document()
                            .append("uid", uuid)
                            .append("name", filename)
                            .append("$currentDate", new Document("lastModified", true))
                            .append("state", "pending")
                            .append("blocks", new String[]{})
            );
        }
        catch (Exception e) {
            System.out.println("file exist in DedupFs");
            return false;
        }
        /* check file */
        List <Pair> resHash = Config.dedup.hash(io);
        LinkedList <String> list = new LinkedList<String>();
        for (Pair pair: resHash) {
            /* check db */
            Document doc = Mongo.mongodb.getCollection(Config.BlockConnection).findOneAndUpdate(
                    new Document("hash", pair.hash),
                    new Document("$inc", new Document("referCnt", 1))
            );
            //no such block
            if (doc == null) {
                //insert into hdfs
                String tmpFile = filename + "_" + Util.uuid();
                /* TODO split io */
                Hdfs.single().create(tmpFile, io);
                //check db again
                try {
                    Mongo.mongodb.getCollection(Config.BlockConnection).insertOne(
                            new Document()
                                    .append("name", tmpFile)
                                    .append("hash", pair.hash)
                                    .append("$currentDate", new Document("lastModified", true))
                                    .append("referCnt", 1)
                    );
                }
                catch (Exception e) {
                    //delete file
                    //find and inc again
                    doc = Mongo.mongodb.getCollection(Config.BlockConnection).findOneAndUpdate(
                            new Document("hash", pair.hash),
                            new Document("$inc", new Document("referCnt", 1))
                    );
                    //delete
                    Hdfs.single().delete(tmpFile);
                    tmpFile = (String)doc.get("name");
                }
                //add block filename
                list.add(tmpFile);
            }
            else
                //add hash to block
                list.add((String)doc.get("name"));
        }
        //update namespace
        try {
            Document doc = Mongo.mongodb.getCollection(Config.FileConnection).findOneAndUpdate(
                    new Document("uid", uuid),
                    new Document()
                            .append("state", "done")
                            .append("$currentDate", new Document("lastModified", true))
                            .append("blocks", list.toArray(new String[]{}))
            );
            if (doc == null)
                return false;
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;

    }
    public static boolean delete(String filename) {
        Document doc = Mongo.mongodb.getCollection(Config.FileConnection).findOneAndDelete(
                new Document()
                    .append("state", "done")
                    .append("name", filename)
        );
        if (doc == null) {
            System.out.println("no such file");
            return false;
        }
        //decrease referrence count
        String [] blocks = (String [])doc.get("blocks");
        for (int i = 0; i < blocks.length; ++i) {
            String block = blocks[i];
            try {
                Mongo.mongodb.getCollection(Config.BlockConnection).findOneAndUpdate(
                        new Document("name", block),
                        new Document("$inc", new Document("referCnt", -1))
                );
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }
    public static LinkedList <String> ls() {
        FindIterable <Document> itr = Mongo.mongodb.getCollection(Config.FileConnection).find(
                new Document("state", "done")
        );
        LinkedList <String> res = new LinkedList<String>();
        for (Document doc: itr) {
            res.add((String)doc.get("name"));
        }
        return res;
    }
}
