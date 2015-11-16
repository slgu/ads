package dedup;

/**
 * Created by slgu1 on 11/14/15.
 */
public class Pair {
    public long idx;
    public String hash;
    public Pair(long idx, String hash) {
        this.idx = idx;
        this.hash = hash;
    }

    @Override
    public String toString() {
        return String.format("idx:%d hash:%s", idx, hash);
    }
}