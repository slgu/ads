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
import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by slgu1 on 11/13/15.
 */
public abstract class FileOp {
    public abstract boolean create(String filename, InputStream io, String absoluteName);
    public abstract boolean delete(String filename);
    public abstract LinkedList <String> ls();
    public abstract boolean get(String filename);
    public double size() {
        return 0;
    }
    private int maxn = 4 * 1024;
    private byte [] buffer = new byte[maxn];
    protected void skip(InputStream io, int count) throws IOException{
        while (true) {
            int byteNeed = count;
            if (byteNeed > maxn)
                byteNeed = maxn;
            int byteRead = io.read(buffer, 0, byteNeed);
            if (byteRead < 0)
                break;
            count -= byteRead;
            if (count == 0)
                break;
        }
    }
}
