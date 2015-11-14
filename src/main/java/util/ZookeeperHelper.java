package util;

import config.Config;
import org.apache.zookeeper.*;

import java.io.IOException;

public class ZookeeperHelper {

    public void readLock() {

    }

    public void writeLock() {

    }

    public ZookeeperHelper() {

    }

    public static void main(String [] args) throws IOException{
        ZooKeeper zk = new ZooKeeper(Config.ZookeeperUrl, 100000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("generate" + watchedEvent.toString());
            }
        });
        try {
            zk.create("/locknode/guid-lock-", new byte[]{'a','b','c'}, null, CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        catch (InterruptedException e) {
            e.printStackTrace();

        }
        catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}