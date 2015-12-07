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
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

//stream when calculating hash
public class HdfsStreamOp extends HdfsOp {
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
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("file exist in DedupFs");
            return false;
        }
        /* check file */
        List<Pair> resHash = null;
        System.out.println("begin hash");
        double fileSize = 0;
        final BlockingQueue<Pair> boundaryQueue = new LinkedBlockingDeque<Pair>();
        //2 io
        InputStream io2;
        //store result blockName
        LinkedList<String> list = new LinkedList<String>();
        long beginIdx = 0;
        try {
            io.mark(0);
            io2 = new BufferedInputStream(new FileInputStream(absoluteName));
            //set bnoundary queue
            Config.dedup.setBoundaryQueue(boundaryQueue);
            Config.dedup.setIo(io);
            new Thread() {
                @Override
                public void run() {
                    try {
                        Config.dedup.hash();
                    } catch (Exception e) {
                    }
                }
            }.start();
            //receive from blocking queue
            while (true) {
                try {
                    Pair pair = boundaryQueue.take();
                    //EOF
                    if (pair.idx == -1) break;
                    fileSize = pair.idx;
                    /* check db */
                    Document doc = Mongo.mongodb.getCollection(Config.BlockConnection).findOneAndUpdate(
                            new Document("hash", pair.hash),
                            new Document("$inc", new Document("referCnt", 1))
                    );
                    //no such block
                    if (doc == null) {
                        //insert into hdfs
                        String tmpFile = filename + "_" + Util.uuid();
                        System.out.println(pair.idx);
                        //get size unit: MB
                        double blockSize = 1.0 * (pair.idx - beginIdx) / 1024 / 1024;
                        //hdfs add error
                        if (!Hdfs.single().create(tmpFile, io2, pair.idx - beginIdx)) {
                            System.out.println("fuck");
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
                        } catch (Exception e) {
                            //delete file
                            //find and inc again
                            doc = Mongo.mongodb.getCollection(Config.BlockConnection).findOneAndUpdate(
                                    new Document("hash", pair.hash),
                                    new Document("$inc", new Document("referCnt", 1))
                            );
                            //delete
                            System.out.println("fuckkkkk");
                            Hdfs.single().delete(tmpFile);
                            tmpFile = (String) doc.get("name");
                        }
                        //add block filename
                        list.add(tmpFile);
                    } else {
                        //add hash to block
                        list.add((String) doc.get("name"));
                        try {
                            //skip this range
                            skip(io2, (int) (pair.idx - beginIdx));
                        } catch (Exception e) {

                        }
                    }
                    //cnmb's bug
                    beginIdx = pair.idx;
                } catch (Exception e) {
                    break;
                }
            }
            fileSize = fileSize * 1.0 / 1024 / 1024;
            io.close();
            io2.close();
        } catch (IOException e) {
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
}
