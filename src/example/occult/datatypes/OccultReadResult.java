package example.occult.datatypes;

import java.util.Map;

public class OccultReadResult {

    private int shardId;
    private Map<Integer, Integer> deps;
    private int shardStamp;
    private boolean isMaster;

    public OccultReadResult(
            int shardId,
            Map<Integer, Integer> deps,
            int shardStamp,
            boolean isSlave) {
        this.shardId = shardId;
        this.deps = deps;
        this.shardStamp = shardStamp;
        this.isMaster = isSlave;
    }

    public int getShardId() {
        return shardId;
    }

    public Map<Integer, Integer> getDeps() {
        return deps;
    }

    public int getShardStamp() {
        return shardStamp;
    }

    public boolean isMaster() {
        return isMaster;
    }
}
