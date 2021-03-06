package operation;

/**
 * Created by slgu1 on 12/5/15.
 */
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import config.Config;
import dedup.Pair;
import org.bson.Document;
import util.Hdfs;
import util.Mongo;
import util.Util;

import javax.print.Doc;
import java.io.*;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
public class HdfsOp extends FileOp{
    private double total = 0;

    @Override
    public double size() {
        return total;
    }


    public boolean create(String filename, InputStream io, String absoluteName) {
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
        double fileSize = 0;
        try {
            io.mark(0);
            Config.dedup.setIo(io);
            resHash = Config.dedup.hash();
            fileSize = resHash.get(resHash.size() - 1).idx * 1.0 / 1024 / 1024;
            io.close();
            io = new BufferedInputStream(new FileInputStream(absoluteName));
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
                //get size unit: MB
                double blockSize = 1.0 * (pair.idx - beginIdx) / 1024 / 1024;
                //hdfs add error
                if (!Hdfs.single().create(tmpFile, io, pair.idx - beginIdx)) {
                    return false;
                }
                //add to total
                total += blockSize;
                //check db again
                try {
                    Mongo.mongodb.getCollection(Config.BlockConnection).insertOne(
                            new Document()
                                    .append("name", tmpFile)
                                    .append("hash", pair.hash)
                                    .append("lastModified", new Date())
                                    .append("referCnt", 1)
                                    .append("size", blockSize)
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
                    System.out.println("fuckkkkk");
                    Hdfs.single().delete(tmpFile);
                    tmpFile = (String)doc.get("name");
                }
                //add block filename
                list.add(tmpFile);
            }
            else {
                //add hash to block
                list.add((String) doc.get("name"));
                try {
                    skip(io, (int)(pair.idx - beginIdx));
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            //cnmb's bug
            beginIdx = pair.idx;
        }
        //close io
        try {
            io.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        //update namespace
        try {
            Document doc = Mongo.mongodb.getCollection(Config.FileConnection).findOneAndUpdate(
                    new Document("uid", uuid),
                    new Document()
                            .append("$currentDate", new Document("lastModified", true))
                            .append("$set", new Document("size", fileSize).append("blocks",list).append("state", "done"))
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
    public boolean delete(String filename) {
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
    public LinkedList <String> ls() {
        FindIterable <Document> itr = Mongo.mongodb.getCollection(Config.FileConnection).find(
                new Document("state", "done")
        );
        LinkedList <String> res = new LinkedList<String>();
        for (Document doc: itr) {
            res.add((String) doc.get("name"));
        }
        return res;
    }
    public boolean get(String filename) {
        Document doc = Mongo.mongodb.getCollection(Config.FileConnection).findOneAndUpdate(
                new Document()
                        .append("state", "done")
                        .append("name", filename),
                //update view time
                new Document()
                        .append("$set", new Document("lastModified", new Date()))
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
        String [] blocks = new String[]{};
        blocks = ((ArrayList <String>)doc.get("blocks")).toArray(blocks);
        byte [] buffer = new byte[1024 * 16];
        int cnt = 0;
        for (String block: blocks) {
            ++cnt;
            Document block_doc = Mongo.mongodb.getCollection(Config.BlockConnection).findOneAndUpdate(
                    new Document("name", block),
                    new Document()
                            .append("$set", new Document("lastModified", new Date()))
            );
            InputStream io = null;
            try {
                io = Hdfs.single().read(block);
            }
            catch (IOException e) {
                e.printStackTrace();
                return false;
            }

            /*
            //DEBUG hash check
            MessageDigest md = null;

            try {
                md = MessageDigest.getInstance("SHA-1");
            }catch (java.security.NoSuchAlgorithmException e){
                e.printStackTrace();
            }

            try {
                String hash = (String) block_doc.get("hash");
                double size = (Double) block_doc.get("size");
                int avail = io.available();
                byte[] buffer_debug = new byte[avail];
                double size2 = avail * 1.0 / 1024 / 1024;
                int offset = 0;
                while (true) {
                    int byteRead = io.read(buffer_debug, offset, avail - offset);
                    if (byteRead < 0)
                        break;
                    offset += byteRead;
                }
                md.update(buffer_debug);
                String another_hash = Util.eraseGarble(md.digest());
                if (!hash.equals(another_hash)) {
                    System.out.println(cnt);
                    System.out.println((String) block_doc.get("name"));
                }
                io.close();
            }
            catch (IOException e) {

            }
            */
            try {
                while (true) {
                    int byteRead = io.read(buffer);
                    if (byteRead < 0)
                        break;
                    //write to output
                    out.write(buffer, 0, byteRead);
                }
                //important to close
                io.close();
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

    public static void debug() {

    }

    public static void main(String [] args) {
        HdfsOp op = new HdfsOp();
        System.out.println(op.ls());
        op.get("w4118_5.ova");
        System.out.println("done");
    }
}