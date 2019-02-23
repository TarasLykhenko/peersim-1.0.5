package example.capstone.datatypes;

import java.util.HashMap;
import java.util.Map;

public class ReadResult {

    private Map<Integer, Integer> localDataClock;

    public ReadResult(Map<Integer, Integer> localDataClock) {
        this.localDataClock = new HashMap<>(localDataClock);
    }

    public Map<Integer, Integer> getLocalDataClock() {
        return localDataClock;
    }
}
