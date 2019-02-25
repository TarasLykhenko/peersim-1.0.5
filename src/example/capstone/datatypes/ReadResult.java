package example.capstone.datatypes;

import java.util.HashMap;
import java.util.Map;

public class ReadResult {

    private Map<Long, Integer> localDataClock;

    public ReadResult(Map<Long, Integer> localDataClock) {
        this.localDataClock = new HashMap<>(localDataClock);
    }

    public Map<Long, Integer> getLocalDataClock() {
        return localDataClock;
    }
}
