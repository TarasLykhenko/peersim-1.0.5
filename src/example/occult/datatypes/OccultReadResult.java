package example.occult.datatypes;

import java.util.Map;

public class OccultReadResult {

    private final int shardId;
    private final Map<Integer, Integer> deps;
    private final int catchAll;
    private final int shardStamp;
    private final boolean isMaster;

    public OccultReadResult(
            int shardId,
            Map<Integer, Integer> deps,
            int catchAll,
            int shardStamp,
            boolean isSlave) {
        this.shardId = shardId;
        this.catchAll = catchAll;
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

    public int getCatchAll() {
        return catchAll;
    }

    public int getShardStamp() {
        return shardStamp;
    }

    public boolean isMaster() {
        return isMaster;
    }
}
