package operation;

import config.Config;
import dedup.Pair;
import org.apache.hadoop.util.hash.Hash;
import org.bson.Document;
import util.Hdfs;
import util.Mongo;
import util.Util;

import java.io.*;
import java.util.*;

/**
 * Created by slgu1 on 12/5/15.
 */
public class DebugOp  extends FileOp{
    //for storage of hash map
    Map <String, Double> mp = new HashMap<String, Double>();
    public boolean create(String filename, InputStream io, String absoluteName) {
        System.out.println("begin hash");
        List <Pair> resHash = null;
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
        System.out.println(String.format("file:%s, size:%f", filename, fileSize));
        LinkedList <String> list = new LinkedList<String>();
        long beginIdx = 0;
        for (Pair pair: resHash) {
            double thisSize = (pair.idx - beginIdx) * 1.0 / 1024 / 1024;
            if (mp.containsKey(pair.hash)) {
                System.out.println(mp.get(pair.hash) + " " + thisSize);
            }
            else {
                mp.put(pair.hash, thisSize);
            }
            beginIdx = pair.idx;
        }
        return true;
    }

    public double size() {
        double res = 0;
        for (double val: mp.values()) {
            res += val;
        }
        return res;
    }

    public boolean delete(String filename) {
        return false;
    }

    public LinkedList<String> ls() {
        return null;
    }

    public boolean get(String filename) {
        return false;
    }
}
