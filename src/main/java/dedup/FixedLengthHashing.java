package dedup;

import util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by voodooinnng on 2015/11/15.
 */
public class FixedLengthHashing extends DedupInterface {
    private long blockSize;
    private byte[] buffer;
    static private final int bufferSize = 4096;
    public FixedLengthHashing(long blockSize) throws Exception
    {
        this.blockSize = blockSize;
        if (blockSize < bufferSize)
            throw new Exception();
        buffer = new byte[bufferSize];
    }
    public List<Pair> hash() throws IOException{
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
           if(length + bytesRead < blockSize) {
               md.update(buffer,0,bytesRead);
               length += bytesRead;
           }
            else {
               md.update(buffer,0,(int)(blockSize - length));
               index++;
               Pair pair = new Pair(index * blockSize, Util.eraseGarble(md.digest()));
               re.add(pair);
               boundaryQueue.add(pair);
               if(length + bytesRead - blockSize != 0)
                   md.update(buffer,(int)(blockSize - length), bytesRead);
               length = length + bytesRead - blockSize;
           }
        }
        if(length != 0) {
            Pair pair = new Pair(length + index * blockSize, Util.eraseGarble(md.digest()));
            re.add(pair);
            boundaryQueue.add(pair);
        }
        boundaryQueue.add(new Pair(-1,""));
        return re;
    }
}