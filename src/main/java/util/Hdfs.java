package util;


import config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by slgu1 on 11/14/15.
 */
//TODO
public class Hdfs {
    private Configuration conf ;
    private FileSystem hdfs;
    private byte[] buffer;
    static private final int bufferSize = 4096;
    public Hdfs(String uri) throws IOException{
        conf = new Configuration ();
        hdfs = FileSystem.get (conf);
        buffer = new byte[bufferSize];
    }
    public Hdfs(String uri, Configuration conf) throws IOException{
        this.conf = conf;
        hdfs = FileSystem.get (conf);
        buffer = new byte[bufferSize];
    }

    public static Hdfs single() {
        return null;
    }

    public boolean create(String filename, InputStream io){
        try {
            FSDataOutputStream os = hdfs.create(new Path(filename));
            while (true) {
                int byteRead = io.read(buffer);
                if (byteRead < 0)
                    break;
                os.write(buffer, 0, byteRead);
            }
        }
        catch (IOException e) {
            return true;
        }
        return false;
    }

    public boolean delete(String filename) {
        try {
            return hdfs.delete(new Path(filename), false);
        }
        catch (IOException e) {
            return false;
        }
    }
    public InputStream read(String filename) throws IOException{
        FSDataInputStream in = hdfs.open(new Path(filename));
        return in;
    }
}
