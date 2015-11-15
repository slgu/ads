package client;

import com.sun.tools.doclets.internal.toolkit.util.DocFinder;
import operation.FileOp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

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
        InputStream io = null;
        if (option.equals("-put")) {
            try {
                io = new FileInputStream(filename);
            }
            catch (IOException e) {
                e.printStackTrace();
                return;
            }
            boolean res = FileOp.create(filename, io);
            if (!res) {
                System.out.println("create fail");
                return;
            }
        }
        else if (option.equals("-rm")) {
            boolean res = FileOp.delete(filename);
            if (!res) {
                System.out.println("delete fail");
                return;
            }
        }
        else if (option.equals("-get")) {
            boolean res = FileOp.get(filename);
            if (!res) {
                System.out.println("get fail");
                return;
            }
        }
        else if (option.equals("-ls")) {
            System.out.println(FileOp.ls());
            return;
        }
        else {
            System.out.println("usage dedup -rm/get/put/ls file.txt");
            return;
        }
    }
}