package dedup;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by slgu1 on 11/14/15.
 */
/*
    TODO: 3 sub-class to be implemented
 */
public interface DedupInterface {
    public List <Pair> hash(InputStream io) throws IOException;
}