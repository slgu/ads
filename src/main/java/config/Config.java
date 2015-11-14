package config;

import dedup.DedupInterface;

/**
 * Created by slgu1 on 11/13/15.
 */
public class Config {
    public static final String ZookeeperUrl = "45.79.147.213:2181";
    public static final String MongoIp = "45.79.147.213";
    public static final int MongoPort = 27017;
    public static final String MongoDb = "ads";
    public static final String FileConnection = "files";
    public static final String BlockConnection = "blocks";
    /* TODO set to be our dedup class */
    public static final DedupInterface dedup = null;
    public static final int sleepTime = 300;
}