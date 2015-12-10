package dedup;

import util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by voodooinnng on 2015/11/15.
 */
public class FileHashing extends DedupInterface{
    private byte[] buffer = new byte[bufferSize];
    static private final int bufferSize = 4096;
    public List<Pair> hash() throws IOException
    {
        int bytesRead = 0;
        MessageDigest md = null;
        ArrayList<Pair> re = new ArrayList<Pair>();
        int index = 0;
        try {
            md = MessageDigest.getInstance("SHA-1");
        }catch (java.security.NoSuchAlgorithmException e){
            e.printStackTrace();
            return null;
        }
        long length = 0;
        while(true) {
            bytesRead = io.read(buffer);
            if(bytesRead < 0)
                break;
            length += bytesRead;
            md.update(buffer,0,bytesRead);
        }
        byte [] thedigest = md.digest();
        Pair pair = new Pair(length, Util.eraseGarble(thedigest));
        re.add(pair);
        boundaryQueue.add(pair);
        boundaryQueue.add(new Pair(-1, ""));
        return re;
    }
}
