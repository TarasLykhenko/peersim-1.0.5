package example.occult.datatypes;

import java.util.Map;

public class OccultMasterWrite {

    private Map<Integer, Integer> deps;
    private int shardStamp;

    public OccultMasterWrite(Map<Integer, Integer> deps, int shardStamp) {
        this.deps = deps;
        this.shardStamp = shardStamp;
    }

    public Map<Integer, Integer> getDeps() {
        return deps;
    }

    public int getShardStamp() {
        return shardStamp;
    }
}
