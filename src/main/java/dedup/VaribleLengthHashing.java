package dedup;
import util.Util;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;


/*refer http://sourceforge.net/projects/rabinhash/ */

public class VaribleLengthHashing extends DedupInterface implements Serializable{

    private static final long serialVersionUID = -7974881488259731932L;
    private final static int P_DEGREE = 64;
    private final static int READ_BUFFER_SIZE = 2048;
    private final static int X_P_DEGREE = 1 << (P_DEGREE - 1);
    private int windowSize = 48;

    private final byte[] buffer;
    public long mask = 0x3FFFFF;
    //private long POLY = Long.decode("0x0060034000F0D50A").longValue();
    private long POLY = Long.decode("0x004AE1202C306041").longValue() | 1<<63;

    private final long[] table32, table40, table48, table54;
    private final long[] table62, table70, table78, table84;
    /**
     *  Constructor for the RabinHashFunction64 object
     *
     */
    public void setMask(long mask)
    {
        this.mask = mask;
    }
    public VaribleLengthHashing(long mask) {
        setMask(mask);
        table32 = new long[256];
        table40 = new long[256];
        table48 = new long[256];
        table54 = new long[256];
        table62 = new long[256];
        table70 = new long[256];
        table78 = new long[256];
        table84 = new long[256];
        buffer = new byte[READ_BUFFER_SIZE];
        long[] mods = new long[P_DEGREE];
        mods[0] = POLY;
        for (int i = 0; i < 256; i++) {
            table32[i] = 0;
            table40[i] = 0;
            table48[i] = 0;
            table54[i] = 0;
            table62[i] = 0;
            table70[i] = 0;
            table78[i] = 0;
            table84[i] = 0;
        }
        for (int i = 1; i < P_DEGREE; i++) {
            mods[i] = mods[i - 1] << 1;
            if ((mods[i - 1] & X_P_DEGREE) != 0) {
                mods[i] = mods[i] ^ POLY;
            }
        }
        for (int i = 0; i < 256; i++) {
            long c = i;
            for (int j = 0; j < 8 && c != 0; j++) {
                if ((c & 1) != 0) {
                    table32[i] = table32[i] ^ mods[j];
                    table40[i] = table40[i] ^ mods[j + 8];
                    table48[i] = table48[i] ^ mods[j + 16];
                    table54[i] = table54[i] ^ mods[j + 24];
                    table62[i] = table62[i] ^ mods[j + 32];
                    table70[i] = table70[i] ^ mods[j + 40];
                    table78[i] = table78[i] ^ mods[j + 48];
                    table84[i] = table84[i] ^ mods[j + 56];
                }
                c >>>= 1;
            }
        }
    }


    public long hash(byte[] A) {
        return hash(A, 0, A.length, 0);
    }


    private long hash(byte[] A, int offset, int length, long ws) {
        long w = ws;
        int start = length % 8;
        for (int s = offset; s < offset + start; s++) {
            w = (w << 8) ^ (A[s] & 0xFF);
        }
        for (int s = offset + start; s < length + offset; s += 8) {
            w =
                    table32[(int) (w & 0xFF)]
                            ^ table40[(int) ((w >>> 8) & 0xFF)]
                            ^ table48[(int) ((w >>> 16) & 0xFF)]
                            ^ table54[(int) ((w >>> 24) & 0xFF)]
                            ^ table62[(int) ((w >>> 32) & 0xFF)]
                            ^ table70[(int) ((w >>> 40) & 0xFF)]
                            ^ table78[(int) ((w >>> 48) & 0xFF)]
                            ^ table84[(int) ((w >>> 56) & 0xFF)]
                            ^ (long) (A[s] << 56)
                            ^ (long) (A[s + 1] << 48)
                            ^ (long) (A[s + 2] << 40)
                            ^ (long) (A[s + 3] << 32)
                            ^ (long) (A[s + 4] << 24)
                            ^ (long) (A[s + 5] << 16)
                            ^ (long) (A[s + 6] << 8)
                            ^ (long) (A[s + 7]);
        }
        return w;
    }

    public long hash(char[] A) {
        long w = 0;
        int start = A.length % 4;
        for (int s = 0; s < start; s++) {
            w = (w << 16) ^ (A[s] & 0xFFFF);
        }
        for (int s = start; s < A.length; s += 4) {
            w =
                    table32[(int) (w & 0xFF)]
                            ^ table40[(int) ((w >>> 8) & 0xFF)]
                            ^ table48[(int) ((w >>> 16) & 0xFF)]
                            ^ table54[(int) ((w >>> 24) & 0xFF)]
                            ^ table62[(int) ((w >>> 32) & 0xFF)]
                            ^ table70[(int) ((w >>> 40) & 0xFF)]
                            ^ table78[(int) ((w >>> 48) & 0xFF)]
                            ^ table84[(int) ((w >>> 56) & 0xFF)]
                            ^ ((long) (A[s] & 0xFFFF) << 48)
                            ^ ((long) (A[s + 1] & 0xFFFF) << 32)
                            ^ ((long) (A[s + 2] & 0xFFFF) << 16)
                            ^ ((long) (A[s + 3] & 0xFFFF));
        }
        return w;
    }

    public List<Pair> hash() throws IOException {
        long hashValue = 0;
        int bytesRead = 0;
        MessageDigest md = null;
        ArrayList<Pair> re = new ArrayList<Pair>();
        long index = 0;
        long lastIndex = 0;
        try {
            md = MessageDigest.getInstance("SHA-1");
        }catch (java.security.NoSuchAlgorithmException e){
            e.printStackTrace();
            return null;
        }
        int spos = 0, length = 0;
        int all = 0, ao = 0;
        synchronized (buffer) {
            while(true)
            {
                int b2 = 0;
                int sp = spos + length;
                if(spos + length > buffer.length) {
                    bytesRead = io.read(buffer, (spos+length)% buffer.length, buffer.length - length);
                    if(bytesRead < 0)
                        break;
                }
                else {
                    bytesRead = io.read(buffer, (spos+length), buffer.length - (spos+length));
                    if(bytesRead < 0)
                        break;
                    b2 = io.read(buffer, 0, spos);
                    if(b2 >= 0)
                        bytesRead+=b2;

                }
                length += bytesRead;
                all += bytesRead;
                while(length >= windowSize)
                {
                    if(spos + windowSize < buffer.length)
                        hashValue = hash(buffer,spos ,windowSize,0);
                    else
                    {
                        hashValue = hash(buffer,spos ,buffer.length - spos,0);
                        hashValue = hash(buffer, 0 ,windowSize - buffer.length + spos,hashValue);
                    }
                    if((hashValue & mask) == 1123L)
                    {

                        if(spos + windowSize < buffer.length){
                            md.update(buffer,spos,windowSize);
                            index += windowSize;
                        }

                        //fos.write(buffer, spos, windowSize);
                        else
                        {
                            md.update(buffer,spos ,buffer.length - spos);
                            md.update(buffer, 0 ,windowSize - buffer.length + spos);
                            index += windowSize;
                        }
                        String sha = Util.eraseGarble(md.digest());
                        Pair pair = new Pair(index,sha);
                        if (boundaryQueue != null)
                            boundaryQueue.add(pair);
                        re.add(pair);
                        lastIndex = index;
                        spos = (spos + windowSize)%buffer.length;
                        ao += windowSize;
                        length -= windowSize;
                        //System.out.print(index + " ");
                        // System.out.println(sha);
                    }
                    else
                    {
                        md.update(buffer, spos, 1);
                        index++;
                        ao ++;
                        spos = (spos + 1)%buffer.length;
                        length --;
                    }
                }
                spos = spos%buffer.length;
                if(b2 < 0)
                    break;
            }
            if(spos + length < buffer.length){
                md.update(buffer, spos, length);
                index += length;
            }

            else
            {
                md.update(buffer,spos ,buffer.length - spos);
                md.update(buffer, 0 ,length - buffer.length + spos);
                index += length;
            }
            if( index > lastIndex){
                Pair pair = new Pair(index, Util.eraseGarble(md.digest()));
                if (boundaryQueue != null)
                    boundaryQueue.add(pair);
                re.add(pair);
            }
        }
        //EOF
        boundaryQueue.add(new Pair(-1, ""));
        return re;
    }

    public long hash(int[] A) {
        long w = 0;
        int start = 0;
        if (A.length % 2 == 1) {
            w = A[0] & 0xFFFFFFFF;
            start = 1;
        }
        for (int s = start; s < A.length; s += 2) {
            w =
                    table32[(int) (w & 0xFF)]
                            ^ table40[(int) ((w >>> 8) & 0xFF)]
                            ^ table48[(int) ((w >>> 16) & 0xFF)]
                            ^ table54[(int) ((w >>> 24) & 0xFF)]
                            ^ table62[(int) ((w >>> 32) & 0xFF)]
                            ^ table70[(int) ((w >>> 40) & 0xFF)]
                            ^ table78[(int) ((w >>> 48) & 0xFF)]
                            ^ table84[(int) ((w >>> 56) & 0xFF)]
                            ^ ((long) (A[s] & 0xFFFFFFFF) << 32)
                            ^ (long) (A[s + 1] & 0xFFFFFFFF);
        }
        return w;
    }


    public long hash(long[] A) {
        long w = 0;
        for (int s = 0; s < A.length; s++) {
            w =
                    table32[(int) (w & 0xFF)]
                            ^ table40[(int) ((w >>> 8) & 0xFF)]
                            ^ table48[(int) ((w >>> 16) & 0xFF)]
                            ^ table54[(int) ((w >>> 24) & 0xFF)]
                            ^ table62[(int) ((w >>> 32) & 0xFF)]
                            ^ table70[(int) ((w >>> 40) & 0xFF)]
                            ^ table78[(int) ((w >>> 48) & 0xFF)]
                            ^ table84[(int) ((w >>> 56) & 0xFF)]
                            ^ (A[s]);
        }
        return w;
    }


    public long hash(Object obj) throws IOException {
        return hash((Serializable) obj);
    }


    public long hash(Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            return hash(baos.toByteArray());
        } finally {
            oos.close();
            baos.close();
            oos = null;
            baos = null;
        }
    }


    public long hash(String s) {
        return hash(s.toCharArray());
    }


}
