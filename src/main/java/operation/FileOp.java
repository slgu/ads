package operation;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import config.Config;
import dedup.Pair;
import org.bson.Document;
import util.Hdfs;
import util.Mongo;
import util.Util;

import javax.print.Doc;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by slgu1 on 11/13/15.
 */
public class FileOp {
    public static boolean create(String filename, InputStream io) {
        String uuid = Util.uuid();
        try {
            System.out.println("begin create");
            //handler
            Mongo.mongodb.getCollection(Config.FileConnection).insertOne(
                    new Document()
                            .append("uid", uuid)
                            .append("name", filename)
                            .append("state", "pending")
                            /*IMPORTANT*/
                            .append("blocks", new ArrayList<String>())
                            .append("lastModified", new Date())
            );
            System.out.println("end create");
            //handler
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("file exist in DedupFs");
            return false;
        }
        /* check file */
        List <Pair> resHash = null;
        System.out.println("begin hash");
        try {
            io.mark(0);
            resHash = Config.dedup.hash(io);
            System.out.println(resHash);
            io.reset();
        }
        catch (IOException e)  {
            e.printStackTrace();
            return false;
        }
        LinkedList <String> list = new LinkedList<String>();
        long beginIdx = 0;
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
                System.out.println(pair.idx);
                Hdfs.single().create(tmpFile, io, pair.idx - beginIdx);
                beginIdx = pair.idx;
                //check db again
                try {
                    Mongo.mongodb.getCollection(Config.BlockConnection).insertOne(
                            new Document()
                                    .append("name", tmpFile)
                                    .append("hash", pair.hash)
                                    .append("lastModified", new Date())
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
                            .append("$set", new Document("state", "done"))
                            .append("$currentDate", new Document("lastModified", true))
                            .append("$set", new Document("blocks", list))
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
    public static boolean get(String filename) {
        Document doc = Mongo.mongodb.getCollection(Config.FileConnection).findOneAndUpdate(
                new Document()
                        .append("state", "done")
                        .append("name", filename),
                //update view time
                new Document()
                        .append("lastModified", new Date())
        );
        if (doc == null) {
            System.out.println("file not exists");
            return false;
        }
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(filename);
        }
        catch (Exception e) {
            System.out.println("open file to write error");
            return false;
        }

        String [] blocks = (String [])doc.get("blocks");
        for (String block: blocks) {
            Mongo.mongodb.getCollection(Config.BlockConnection).findOneAndUpdate(
                    new Document("name", block),
                    new Document()
                            .append("lastModified", new Date())
            );
            InputStream io = null;
            try {
                io = Hdfs.single().read(block);
            }
            catch (IOException e) {
                return false;
            }
            try {
                byte[] buffer = new byte[io.available()];
                int byteRead = io.read(buffer);
                //write to output
                out.write(buffer, 0, byteRead);
            }
            catch (IOException e) {
                System.out.println("read error");
                return false;
            }
        }
        try {
            out.close();
        }
        catch (IOException e) {
            System.out.println("close write handle error");
            return false;
        }
        return true;
    }
}