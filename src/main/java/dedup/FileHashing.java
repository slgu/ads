package dedup;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by voodooinnng on 2015/11/15.
 */
public class FileHashing implements DedupInterface{
    private byte[] buffer;
    static private final int bufferSize = 4096;
    public List<Pair> hash(InputStream is) throws IOException
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
            bytesRead = is.read(buffer);
            if(bytesRead < 0)
                break;
            length += bytesRead;
            md.update(buffer,0,bytesRead);
        }
        re.add(new Pair(length,new String(md.digest())));

        return re;
    }
}
