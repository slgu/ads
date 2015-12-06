package dedup;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by slgu1 on 11/14/15.
 */
abstract public class DedupInterface {
    abstract public List <Pair> hash() throws IOException;
    protected BlockingQueue <Pair> boundaryQueue;
    public void setBoundaryQueue(BlockingQueue<Pair> boundaryQueue) {
        this.boundaryQueue = boundaryQueue;
    }
    protected InputStream io;
    public void setIo(InputStream io) {
        this.io = io;
    }
}