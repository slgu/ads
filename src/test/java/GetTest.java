import config.Config;
import operation.HdfsOp;

import java.io.*;

/**
 * Created by slgu1 on 12/8/15.
 */
public class GetTest {
    private static String BackUpImgDir = "/Users/slgu1/ads_data/BackupVm";
    private static String ISOImgDir = "/Users/slgu1/ads_data/ISO";
    public static void get(String dirString) throws IOException {
        HdfsOp op = new HdfsOp();
        System.out.println(op.ls());
        File dir = new File(dirString);
        for (File f: dir.listFiles()) {
            if (f.isFile()) {
                if (f.getName().startsWith("."))
                    continue;
                op.get(f.getName());
            }
        }
    }
    public static void main(String [] args) {
        long nanoBefore = System.nanoTime();
        try {
            get(ISOImgDir);
        }
        catch (IOException e) {

        }
        long nanoAfter = System.nanoTime();
        System.out.println("Time usage: " + 1.0 * (nanoAfter - nanoBefore) / 1000000000);
    }
}