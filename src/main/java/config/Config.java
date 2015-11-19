package config;

import dedup.DedupInterface;
import dedup.FileHashing;
import dedup.FixedLengthHashing;
import dedup.VaribleLengthHashing;

/**
 * Created by slgu1 on 11/5/15.
 */
public class Config {
    public static final String MongoIp = "45.79.147.213";
    public static final int MongoPort = 27017;
    public static final String MongoDb = "ads";
    public static final String FileConnection = "files";
    public static final String BlockConnection = "blocks";
    public static DedupInterface dedup = null;
    static {
        try {
            //dedup = new FixedLengthHashing(1024 * 1024 * 64);
            dedup = new VaribleLengthHashing((1 << 26) - 1);
        }
        catch (Exception e) {
            dedup = new FileHashing();
        }
    }
    public static final int sleepTime = 300;
    /* inner network faster transfer */
    public static final String HdfsUri = "hdfs://10.211.55.3:9000";
    public static final String DedupDir = "/dedup";
}
