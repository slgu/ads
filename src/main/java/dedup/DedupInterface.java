package dedup;

import java.io.InputStream;
import java.util.List;

/**
 * Created by slgu1 on 11/14/15.
 */

public interface DedupInterface {
    public List <Pair> hash(InputStream io);
}
