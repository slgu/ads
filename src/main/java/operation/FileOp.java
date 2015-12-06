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
}
