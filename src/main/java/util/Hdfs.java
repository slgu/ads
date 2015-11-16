package util;


import config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by slgu1 on 11/14/15.
 */
public class Hdfs {
    private Configuration conf ;
    private FileSystem hdfs;
    private byte[] buffer;
    static private final int bufferSize = 4096;
    private static Hdfs HdfsSingle = null;
    public Hdfs(String uri) throws IOException, URISyntaxException{
        conf = new Configuration ();
        hdfs = FileSystem.get(new URI(uri), conf);
        buffer = new byte[bufferSize];
    }
    public Hdfs(String uri, Configuration conf) throws IOException{
        this.conf = conf;
        hdfs = FileSystem.get (conf);
        buffer = new byte[bufferSize];
    }

    public static Hdfs single() {
        /*TODO maybe lock is needed */
        if (HdfsSingle == null) {
            try {
                HdfsSingle = new Hdfs(Config.HdfsUri);
            }
            catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        return HdfsSingle;
    }
    public boolean create(String filename, InputStream io, long number){
        try {
            FSDataOutputStream os = hdfs.create(new Path(Config.DedupDir + "/" + filename));
            while (true) {
                int setMax = bufferSize;
                if (setMax > number)
                    setMax = (int)number;
                int byteRead = io.read(buffer, 0, setMax);
                if (byteRead < 0)
                    break;
                number -= byteRead;
                os.write(buffer, 0, byteRead);
                if (number == 0)
                    break;
            }
            os.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public boolean create(String filename, InputStream io) {
        try {
            FSDataOutputStream os = hdfs.create(new Path(Config.DedupDir + "/" + filename));
            while (true) {
                int byteRead = io.read(buffer);
                if (byteRead < 0)
                    break;
                os.write(buffer, 0, byteRead);
            }
            os.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public boolean delete(String filename) {
        try {
            return hdfs.delete(new Path(Config.DedupDir + "/" + filename), false);
        }
        catch (IOException e) {
            return false;
        }
    }

    public InputStream read(String filename) throws IOException{
        FSDataInputStream in = hdfs.open(new Path(Config.DedupDir + "/" + filename));
        return in;
    }

    public static void main(String [] args) {
        try {
            FileInputStream input = new FileInputStream("/Users/slgu1/Desktop/w4118.ova");
            Hdfs hdfsDao = Hdfs.single();
            hdfsDao.create("w4118", input);
            //System.out.println(hdfsDao.delete("haha"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
