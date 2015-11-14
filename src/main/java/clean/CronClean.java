package clean;

import config.Config;
import org.bson.Document;
import util.Hdfs;
import util.Mongo;

import java.util.Date;

/**
 * Created by slgu1 on 11/13/15.
 */
public class CronClean {
    public static void main(String [] args) {
        while (true) {
            try {
                Thread.sleep(Config.sleepTime * 1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            Date dateAfterThirtyMinute = new Date(new Date().getTime() - 60 * 1000 * 30);
            Document doc = Mongo.mongodb.getCollection(Config.BlockConnection).findOneAndDelete(
                    new Document("state", "done")
                            .append("referCnt", 0)
                            .append("lastModified", new Document("$lt", dateAfterThirtyMinute))
            );
            if (doc ==null)
                continue;
            String filename = (String)doc.get("name");
            //delete fileblock in hdfs
            Hdfs.single().delete(filename);
        }
    }
}
