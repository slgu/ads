package client;

import operation.FileOp;

import java.io.*;

import operation.HdfsOp;
import org.bson.conversions.Bson;

/**
 * Created by slgu1 on 11/13/15.
 */
public class Client {
    //command line option
    /*
     * dedup -put file.txt
     * dedup -rm file.txt
     * dedup -get file.txt
     */
    public static void main(String [] args) {
        if (args.length != 2) {
            System.out.println("wrong input parameter");
            return;
        }
        String option = args[0];
        String filename = args[1];
        FileOp op = new HdfsOp();
        if (option.equals("-put")) {
            InputStream io = null;
            String baseName = null;
            try {
                baseName = new File(filename).getName();
                io = new BufferedInputStream(new FileInputStream(filename));
            }
            catch (IOException e) {
                e.printStackTrace();
                return;
            }
            boolean res = op.create(baseName, io, filename);
            if (!res) {
                System.out.println("create fail");
                return;
            }
        }
        else if (option.equals("-rm")) {
            boolean res = op.delete(filename);
            if (!res) {
                System.out.println("delete fail");
                return;
            }
        }
        else if (option.equals("-get")) {
            boolean res = op.get(filename);
            if (!res) {
                System.out.println("get fail");
                return;
            }
        }
        else if (option.equals("-ls")) {
            System.out.println(op.ls());
            return;
        }
        else {
            System.out.println("usage dedup -rm/get/put/ls file.txt");
            return;
        }
    }
}